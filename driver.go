package chai

import (
	"context"
	"database/sql"
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
	db, err := database.Open(name, &database.Options{
		CatalogLoader: catalogstore.LoadCatalog,
	})
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
	db        *database.Database
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
	db   *database.Database
	conn *database.Connection
}

// Prepare returns a prepared statement, bound to this connection.
func (c *conn) Prepare(q string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), q)
}

// PrepareContext returns a prepared statement, bound to this connection.
func (c *conn) PrepareContext(ctx context.Context, q string) (driver.Stmt, error) {
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

	return stmt{
		pq:   pq,
		conn: c,
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
	err := c.conn.Reset()
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
type stmt struct {
	pq   query.Query
	conn *conn
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

	res, err := s.pq.Run(&query.Context{
		Ctx:    ctx,
		DB:     s.conn.db,
		Conn:   s.conn.conn,
		Params: namedValueToParams(args),
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

	return execResult{}, res.Skip()
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

	res, err := s.pq.Run(&query.Context{
		Ctx:    ctx,
		DB:     s.conn.db,
		Conn:   s.conn.conn,
		Params: namedValueToParams(args),
	})
	if err != nil {
		return nil, err
	}

	return newRows(res)
}

// Close does nothing.
func (s stmt) Close() error {
	return nil
}

type Rows struct {
	res     *statement.Result
	it      database.Iterator
	columns []string
}

func newRows(res *statement.Result) (*Rows, error) {
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
			dest[i] = types.AsString(v)
		case types.TypeBlob:
			dest[i] = types.AsByteSlice(v)
		default:
			panic("unsupported type: " + v.Type().String())
		}
		i++

		return nil
	})
}

func namedValueToParams(args []driver.NamedValue) []environment.Param {
	params := make([]environment.Param, len(args))
	for i, arg := range args {
		var p environment.Param
		p.Name = arg.Name
		p.Value = arg.Value
		params[i] = p
	}

	return params
}
