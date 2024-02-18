package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"sync"
	"time"

	"github.com/chaisql/chai"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

func init() {
	sql.Register("chai", sqlDriver{})
}

var (
	_ driver.Driver        = (*sqlDriver)(nil)
	_ driver.DriverContext = (*sqlDriver)(nil)
)

// sqlDriver is a driver.Driver that can open a new connection to a Chai database.
// It is the driver used to register Chai against the database/sql package.
type sqlDriver struct{}

func (d sqlDriver) Open(name string) (driver.Conn, error) {
	return nil, errors.New("requires go1.10 or greater")
}

func (d sqlDriver) OpenConnector(name string) (driver.Connector, error) {
	db, err := chai.Open(name)
	if err != nil {
		return nil, err
	}

	return &connector{
		db:     db,
		driver: d,
	}, nil
}

var (
	_ driver.Connector = (*connector)(nil)
	_ io.Closer        = (*connector)(nil)
)

type connector struct {
	driver    driver.Driver
	db        *chai.DB
	closeOnce sync.Once
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	cc, err := c.db.Connect()
	if err != nil {
		return nil, err
	}

	return &conn{
		db:   c.db,
		conn: cc,
	}, nil
}

func (c *connector) Driver() driver.Driver {
	return c.driver
}

func (c *connector) Close() error {
	var err error
	c.closeOnce.Do(func() {
		err = c.db.Close()
	})
	return err
}

// conn represents a connection to the Chai database.
// It implements the database/sql/driver.Conn interface.
type conn struct {
	db   *chai.DB
	conn *chai.Connection
}

// Prepare returns a prepared statement, bound to this connection.
func (c *conn) Prepare(q string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), q)
}

// PrepareContext returns a prepared statement, bound to this connection.
func (c *conn) PrepareContext(ctx context.Context, q string) (driver.Stmt, error) {
	s, err := c.conn.Prepare(q)
	if err != nil {
		return nil, err
	}

	return stmt{
		stmt: s,
	}, nil
}

// Close closes any ongoing transaction.
func (c *conn) Close() error {
	return c.conn.Close()
}

// Begin starts and returns a new transaction.
func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *conn) ResetSession(ctx context.Context) error {
	err := c.conn.Conn.Reset()
	if err != nil {
		return driver.ErrBadConn
	}

	return nil
}

// BeginTx starts and returns a new transaction.
// It uses the ReadOnly option to determine whether to start a read-only or read/write transaction.
// If the Isolation option is non zero, an error is returned.
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.Isolation != 0 {
		return nil, errors.New("isolation levels are not supported")
	}

	// if the ReadOnly flag is explicitly specified, create a read-only transaction,
	// otherwise create a read/write transaction.
	return c.conn.Begin(!opts.ReadOnly)
}

// Stmt is a prepared statement. It is bound to a Conn and not
// used by multiple goroutines concurrently.
type stmt struct {
	stmt *chai.Statement
}

// NumInput returns the number of placeholder parameters.
func (s stmt) NumInput() int { return -1 }

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
func (s stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("not implemented")
}

// ExecContext executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
func (s stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return execResult{}, s.stmt.Exec(namedValueToParams(args)...)
}

type execResult struct{}

// LastInsertId is not supported and returns an error.
func (r execResult) LastInsertId() (int64, error) {
	return 0, errors.New("not supported")
}

// RowsAffected is not supported and returns an error.
func (r execResult) RowsAffected() (int64, error) {
	return 0, errors.New("not supported")
}

func (s stmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, errors.New("not implemented")
}

// QueryContext executes a query that may return rows, such as a
// SELECT.
func (s stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	res, err := s.stmt.Query(namedValueToParams(args)...)
	if err != nil {
		return nil, err
	}

	rs := newRows(res)
	rs.columns = res.Columns()
	return rs, nil
}

func namedValueToParams(args []driver.NamedValue) []any {
	params := make([]any, len(args))
	for i, arg := range args {
		var p environment.Param
		p.Name = arg.Name
		p.Value = arg.Value
		params[i] = p
	}

	return params
}

// Close does nothing.
func (s stmt) Close() error {
	return nil
}

var errStop = errors.New("stop")

type Rows struct {
	res      *chai.Result
	cancelFn func()
	c        chan Row
	wg       sync.WaitGroup
	columns  []string
}

type Row struct {
	r   *chai.Row
	err error
}

func newRows(res *chai.Result) *Rows {
	ctx, cancel := context.WithCancel(context.Background())

	rs := Rows{
		res:      res,
		cancelFn: cancel,
		c:        make(chan Row),
	}
	rs.wg.Add(1)

	go rs.iterate(ctx)

	return &rs
}

func (rs *Rows) iterate(ctx context.Context) {
	defer rs.wg.Done()
	defer close(rs.c)

	select {
	case <-ctx.Done():
		return
	case <-rs.c:
	}

	err := rs.res.Iterate(func(r *chai.Row) error {
		select {
		case <-ctx.Done():
			return errStop
		case rs.c <- Row{
			r: r,
		}:

			select {
			case <-ctx.Done():
				return errStop
			case <-rs.c:
				return nil
			}
		}
	})

	if errors.Is(err, errStop) || err == nil {
		return
	}
	if err != nil {
		rs.c <- Row{
			err: err,
		}
		return
	}
}

// Columns returns the fields selected by the SELECT statement.
func (rs *Rows) Columns() []string {
	return rs.res.Columns()
}

// Close closes the rows iterator.
func (rs *Rows) Close() error {
	rs.cancelFn()
	rs.wg.Wait()
	return rs.res.Close()
}

func (rs *Rows) Next(dest []driver.Value) error {
	rs.c <- Row{}

	row, ok := <-rs.c
	if !ok {
		return io.EOF
	}

	if row.err != nil {
		return row.err
	}

	for i := range rs.columns {
		if rs.columns[i] == "*" {
			dest[i] = row.r

			continue
		}

		tp, err := row.r.GetColumnType(rs.columns[i])
		if err != nil {
			return err
		}
		switch tp {
		case types.TypeBoolean.String():
			var b bool
			err = row.r.ScanColumn(rs.columns[i], &b)
			if err != nil {
				return err
			}
			dest[i] = b
		case types.TypeInteger.String():
			var ii int32
			err = row.r.ScanColumn(rs.columns[i], &ii)
			if err != nil {
				return err
			}
			dest[i] = ii
		case types.TypeBigint.String():
			var bi int64
			err = row.r.ScanColumn(rs.columns[i], &bi)
			if err != nil {
				return err
			}
		case types.TypeDouble.String():
			var d float64
			err = row.r.ScanColumn(rs.columns[i], &d)
			if err != nil {
				return err
			}
			dest[i] = d
		case types.TypeTimestamp.String():
			var t time.Time
			err = row.r.ScanColumn(rs.columns[i], &t)
			if err != nil {
				return err
			}
			dest[i] = t
		case types.TypeText.String():
			var s string
			err = row.r.ScanColumn(rs.columns[i], &s)
			if err != nil {
				return err
			}
			dest[i] = s
		case types.TypeBlob.String():
			var b []byte
			err = row.r.ScanColumn(rs.columns[i], &b)
			if err != nil {
				return err
			}
			dest[i] = b
		default:
			err = row.r.ScanColumn(rs.columns[i], dest[i])
			if err != nil {
				return err
			}
		}
	}

	return nil
}

type valueScanner struct {
	dest any
}

func (v valueScanner) Scan(src any) error {
	if r, ok := src.(*chai.Row); ok {
		return r.StructScan(v.dest)
	}

	vv, err := row.NewValue(src)
	if err != nil {
		return err
	}

	return row.ScanValue(vv, v.dest)
}

// Scanner turns a variable into a sql.Scanner.
// x must be a pointer to a valid variable.
func Scanner(x any) sql.Scanner {
	return valueScanner{x}
}
