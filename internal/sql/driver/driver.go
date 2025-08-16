package driver

import (
	"context"
	"database/sql/driver"
	"io"
	"sync"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/database/catalogstore"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/query"
	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

var (
	_ driver.Driver        = (*Driver)(nil)
	_ driver.DriverContext = (*Driver)(nil)
)

// Driver is a driver.Driver that can open a new connection to a Chai database.
// It is the driver used to register Chai against the database/sql package.
type Driver struct{}

func (d Driver) Open(name string) (driver.Conn, error) {
	return nil, errors.New("requires go1.10 or greater")
}

func (d Driver) OpenConnector(name string) (driver.Connector, error) {
	db, err := database.Open(name, &database.Options{
		CatalogLoader: catalogstore.LoadCatalog,
	})
	if err != nil {
		return nil, err
	}

	return &Connector{
		db:     db,
		driver: d,
	}, nil
}

var (
	_ driver.Connector = (*Connector)(nil)
	_ io.Closer        = (*Connector)(nil)
)

type Connector struct {
	driver    driver.Driver
	db        *database.Database
	closeOnce sync.Once
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	cc, err := c.db.Connect()
	if err != nil {
		return nil, err
	}

	return &Conn{
		db:   c.db,
		conn: cc,
	}, nil
}

func (c *Connector) Driver() driver.Driver {
	return c.driver
}

func (c *Connector) Close() error {
	var err error
	c.closeOnce.Do(func() {
		err = c.db.Close()
	})
	return err
}

// Conn represents a connection to the Chai database.
// It implements the database/sql/driver.Conn interface.
type Conn struct {
	db   *database.Database
	conn *database.Connection
}

func (c *Conn) DB() *database.Database {
	return c.db
}

func (c *Conn) Conn() *database.Connection {
	return c.conn
}

// Prepare returns a prepared statement, bound to this connection.
func (c *Conn) Prepare(q string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), q)
}

// PrepareContext returns a prepared statement, bound to this connection.
func (c *Conn) PrepareContext(ctx context.Context, q string) (driver.Stmt, error) {
	pq, err := parser.ParseQuery(q)
	if err != nil {
		return nil, err
	}

	err = pq.Prepare(&query.Context{
		Ctx:  ctx,
		DB:   c.db,
		Conn: c.conn,
	})
	if err != nil {
		return nil, err
	}

	return Stmt{
		pq:   pq,
		conn: c,
	}, nil
}

// Close closes any ongoing transaction.
func (c *Conn) Close() error {
	return c.conn.Close()
}

// Begin starts and returns a new transaction.
func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *Conn) ResetSession(ctx context.Context) error {
	err := c.conn.Reset()
	if err != nil {
		return driver.ErrBadConn
	}

	return nil
}

// BeginTx starts and returns a new transaction.
// It uses the ReadOnly option to determine whether to start a read-only or read/write transaction.
// If the Isolation option is non zero, an error is returned.
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.Isolation != 0 {
		return nil, errors.New("isolation levels are not supported")
	}

	// if the ReadOnly flag is explicitly specified, create a read-only transaction,
	// otherwise create a read/write transaction.
	tx, err := c.conn.BeginTx(&database.TxOptions{
		ReadOnly: opts.ReadOnly,
	})
	if err != nil {
		return nil, err
	}

	return tx, nil
}

// Stmt is a prepared statement. It is bound to a Conn and not
// used by multiple goroutines concurrently.
type Stmt struct {
	pq   query.Query
	conn *Conn
}

// NumInput returns the number of placeholder parameters.
func (s Stmt) NumInput() int { return -1 }

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
func (s Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("not implemented")
}

// ExecContext executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
func (s Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	res, err := s.pq.Run(&query.Context{
		Ctx:    ctx,
		DB:     s.conn.db,
		Conn:   s.conn.conn,
		Params: NamedValueToParams(args),
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		er := res.Close()
		if err == nil {
			err = er
		}
	}()

	return ExecResult{}, res.Skip()
}

type ExecResult struct{}

// LastInsertId is not supported and returns an error.
func (r ExecResult) LastInsertId() (int64, error) {
	return 0, errors.New("not supported")
}

// RowsAffected is not supported and returns an error.
func (r ExecResult) RowsAffected() (int64, error) {
	return 0, errors.New("not supported")
}

func (s Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, errors.New("not implemented")
}

// QueryContext executes a query that may return rows, such as a
// SELECT.
func (s Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	res, err := s.pq.Run(&query.Context{
		Ctx:    ctx,
		DB:     s.conn.db,
		Conn:   s.conn.conn,
		Params: NamedValueToParams(args),
	})
	if err != nil {
		return nil, err
	}

	return NewRows(res)
}

// Close does nothing.
func (s Stmt) Close() error {
	return nil
}

type Rows struct {
	res     *statement.Result
	it      database.Iterator
	columns []string
}

func NewRows(res *statement.Result) (*Rows, error) {
	columns, err := res.Columns()
	if err != nil {
		return nil, err
	}

	it, err := res.Iterator()
	if err != nil {
		return nil, err
	}

	return &Rows{
		res:     res,
		it:      it,
		columns: columns,
	}, nil
}

// Columns returns the fields selected by the SELECT statement.
func (rs *Rows) Columns() []string {
	return rs.columns
}

// Close closes the rows iterator.
func (rs *Rows) Close() error {
	var errs []error
	if rs.it != nil {
		if err := rs.it.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := rs.res.Close(); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (rs *Rows) Next(dest []driver.Value) error {
	if rs.it == nil || !rs.it.Next() {
		return io.EOF
	}

	r, err := rs.it.Row()
	if err != nil {
		return err
	}
	var i int
	return r.Iterate(func(column string, v types.Value) error {
		switch v.Type() {
		case types.TypeNull:
			dest[i] = nil
		case types.TypeBoolean:
			dest[i] = types.AsBool(v)
		case types.TypeInteger:
			dest[i] = types.AsInt32(v)
		case types.TypeBigint:
			dest[i] = types.AsInt64(v)
		case types.TypeDouble:
			dest[i] = types.AsFloat64(v)
		case types.TypeTimestamp:
			dest[i] = types.AsTime(v)
		case types.TypeText:
			// Make a copy of the string to avoid issues with re-use.
			s := types.AsString(v)
			cp := make([]byte, len(s))
			copy(cp, s)
			dest[i] = string(cp)
		case types.TypeBlob:
			// Make a copy of the byte slice to avoid issues with re-use.
			b := types.AsByteSlice(v)
			cp := make([]byte, len(b))
			copy(cp, b)
			dest[i] = cp
		default:
			panic("unsupported type: " + v.Type().String())
		}
		i++

		return nil
	})
}

func NamedValueToParams(args []driver.NamedValue) []environment.Param {
	params := make([]environment.Param, len(args))
	for i, arg := range args {
		var p environment.Param
		p.Name = arg.Name
		p.Value = arg.Value
		params[i] = p
	}

	return params
}
