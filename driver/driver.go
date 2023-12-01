package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/object"
	"github.com/genjidb/genji/internal/types"
)

func init() {
	sql.Register("genji", sqlDriver{})
}

var (
	_ driver.Driver        = (*sqlDriver)(nil)
	_ driver.DriverContext = (*sqlDriver)(nil)
)

// sqlDriver is a driver.Driver that can open a new connection to a Genji database.
// It is the driver used to register Genji against the database/sql package.
type sqlDriver struct{}

func (d sqlDriver) Open(name string) (driver.Conn, error) {
	return nil, errors.New("requires go1.10 or greater")
}

func (d sqlDriver) OpenConnector(name string) (driver.Connector, error) {
	db, err := genji.Open(name)
	if err != nil {
		return nil, err
	}

	c := &connector{
		db:     db,
		driver: d,
	}
	runtime.SetFinalizer(c, (*connector).Close)

	return c, nil
}

var (
	_ driver.Connector = (*connector)(nil)
	_ io.Closer        = (*connector)(nil)
)

type connector struct {
	driver driver.Driver

	db *genji.DB

	closeOnce sync.Once
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	return &conn{db: c.db}, nil
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

// conn represents a connection to the Genji database.
// It implements the database/sql/driver.Conn interface.
type conn struct {
	db *genji.DB
	tx *genji.Tx
}

// Prepare returns a prepared statement, bound to this connection.
func (c *conn) Prepare(q string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), q)
}

// PrepareContext returns a prepared statement, bound to this connection.
func (c *conn) PrepareContext(ctx context.Context, q string) (driver.Stmt, error) {
	var s *genji.Statement
	var err error

	if c.tx != nil {
		s, err = c.tx.Prepare(q)
	} else {
		s, err = c.db.Prepare(q)
	}
	if err != nil {
		return nil, err
	}

	return stmt{
		stmt: s,
	}, nil
}

// Close closes any ongoing transaction.
func (c *conn) Close() error {
	if c.tx != nil {
		return c.tx.Rollback()
	}

	return nil
}

// Begin starts and returns a new transaction.
func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts and returns a new transaction.
// It uses the ReadOnly option to determine whether to start a read-only or read/write transaction.
// If the Isolation option is non zero, an error is returned.
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.Isolation != 0 {
		return nil, errors.New("isolation levels are not supported")
	}

	db := c.db.WithContext(ctx)

	// if the ReadOnly flag is explicitly specified, create a read-only transaction,
	// otherwise create a read/write transaction.
	var err error
	c.tx, err = db.Begin(!opts.ReadOnly)

	return c, err
}

func (c *conn) Commit() error {
	err := c.tx.Commit()
	c.tx = nil
	return err
}

func (c *conn) Rollback() error {
	err := c.tx.Rollback()
	c.tx = nil
	return err
}

// Stmt is a prepared statement. It is bound to a Conn and not
// used by multiple goroutines concurrently.
type stmt struct {
	stmt *genji.Statement
}

// NumInput returns the number of placeholder parameters.
func (s stmt) NumInput() int { return -1 }

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
func (s stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("not implemented")
}

// CheckNamedValue has the same behaviour as driver.DefaultParameterConverter, except that
// it allows types.Object to be passed as parameters.
// It implements the driver.NamedValueChecker interface.
func (s stmt) CheckNamedValue(nv *driver.NamedValue) error {
	if _, ok := nv.Value.(types.Object); ok {
		return nil
	}

	if _, ok := nv.Value.(object.Scanner); ok {
		return nil
	}

	var err error
	val, err := driver.DefaultParameterConverter.ConvertValue(nv.Value)
	if err == nil {
		nv.Value = val
		return nil
	}

	return nil
}

// ExecContext executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
func (s stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return result{}, s.stmt.Exec(driverNamedValueToParams(args)...)
}

type result struct{}

// LastInsertId is not supported and returns an error.
func (r result) LastInsertId() (int64, error) {
	return 0, errors.New("not supported")
}

// RowsAffected is not supported and returns an error.
func (r result) RowsAffected() (int64, error) {
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

	res, err := s.stmt.Query(driverNamedValueToParams(args)...)
	if err != nil {
		return nil, err
	}

	rs := newRecordStream(res)
	rs.columns = res.Columns()
	return rs, nil
}

func driverNamedValueToParams(args []driver.NamedValue) []any {
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

type recordStream struct {
	res      *genji.Result
	cancelFn func()
	c        chan row
	wg       sync.WaitGroup
	columns  []string
}

type row struct {
	r   *genji.Row
	err error
}

func newRecordStream(res *genji.Result) *recordStream {
	ctx, cancel := context.WithCancel(context.Background())

	ds := recordStream{
		res:      res,
		cancelFn: cancel,
		c:        make(chan row),
	}
	ds.wg.Add(1)

	go ds.iterate(ctx)

	return &ds
}

func (rs *recordStream) iterate(ctx context.Context) {
	defer rs.wg.Done()
	defer close(rs.c)

	select {
	case <-ctx.Done():
		return
	case <-rs.c:
	}

	err := rs.res.Iterate(func(r *genji.Row) error {
		select {
		case <-ctx.Done():
			return errStop
		case rs.c <- row{
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
		rs.c <- row{
			err: err,
		}
		return
	}
}

// Columns returns the fields selected by the SELECT statement.
func (rs *recordStream) Columns() []string {
	return rs.res.Columns()
}

// Close closes the rows iterator.
func (rs *recordStream) Close() error {
	rs.cancelFn()
	rs.wg.Wait()
	return rs.res.Close()
}

func (rs *recordStream) Next(dest []driver.Value) error {
	rs.c <- row{}

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
		case types.BooleanValue.String():
			var b bool
			err = row.r.ScanColumn(rs.columns[i], &b)
			if err != nil {
				return err
			}
			dest[i] = b
		case types.IntegerValue.String():
			var ii int64
			err = row.r.ScanColumn(rs.columns[i], &ii)
			if err != nil {
				return err
			}
			dest[i] = ii
		case types.DoubleValue.String():
			var d float64
			err = row.r.ScanColumn(rs.columns[i], &d)
			if err != nil {
				return err
			}
			dest[i] = d
		case types.TimestampValue.String():
			var t time.Time
			err = row.r.ScanColumn(rs.columns[i], &t)
			if err != nil {
				return err
			}
			dest[i] = t
		case types.TextValue.String():
			var s string
			err = row.r.ScanColumn(rs.columns[i], &s)
			if err != nil {
				return err
			}
			dest[i] = s
		case types.BlobValue.String():
			var b []byte
			err = row.r.ScanColumn(rs.columns[i], &b)
			if err != nil {
				return err
			}
			dest[i] = b
		case types.ArrayValue.String():
			var a []any
			err = row.r.ScanColumn(rs.columns[i], &a)
			if err != nil {
				return err
			}
			dest[i] = a
		case types.ObjectValue.String():
			m := make(map[string]any)
			err = row.r.ScanColumn(rs.columns[i], &m)
			if err != nil {
				return err
			}
			dest[i] = m
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
	if r, ok := src.(*genji.Row); ok {
		return r.StructScan(v.dest)
	}

	vv, err := object.NewValue(src)
	if err != nil {
		return err
	}

	return object.ScanValue(vv, v.dest)
}

// Scanner turns a variable into a sql.Scanner.
// x must be a pointer to a valid variable.
func Scanner(x any) sql.Scanner {
	return valueScanner{x}
}
