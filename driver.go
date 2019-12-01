package genji

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync"

	"github.com/asdine/genji/document"
	"github.com/asdine/genji/parser"
	"github.com/asdine/genji/query"
)

func init() {
	sql.Register("genji", sqlDriver{})
}

// sqlDriver is a driver.Driver that can open a new connection to a Genji database.
// It is the driver used to register Genji against the database/sql package.
type sqlDriver struct{}

func (d sqlDriver) Open(name string) (driver.Conn, error) {
	db, err := Open(name)
	if err != nil {
		return nil, err
	}

	return &conn{db: db}, nil
}

// proxyDriver is used to turn an existing DB into a driver.Driver.
type proxyDriver struct {
	db *DB
}

func newDriver(db *DB) driver.Driver {
	return proxyDriver{
		db: db,
	}
}

func (d proxyDriver) Open(name string) (driver.Conn, error) {
	return &conn{db: d.db}, nil
}

type proxyConnector struct {
	driver driver.Driver
}

func newProxyConnector(db *DB) driver.Connector {
	return proxyConnector{
		driver: newDriver(db),
	}
}

func (c proxyConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return c.driver.Open("")
}

func (c proxyConnector) Driver() driver.Driver {
	return c.driver
}

// conn represents a connection to the Genji database.
// It implements the database/sql/driver.Conn interface.
type conn struct {
	db            *DB
	tx            *Tx
	nonPromotable bool
}

// Prepare returns a prepared statement, bound to this connection.
func (c *conn) Prepare(q string) (driver.Stmt, error) {
	pq, err := parser.ParseQuery(q)
	if err != nil {
		return nil, err
	}

	return stmt{
		db:            c.db,
		tx:            c.tx,
		q:             pq,
		nonPromotable: c.nonPromotable,
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

	var err error

	// if the ReadOnly flag is explicitly specified, create a non promotable transaction,
	// otherwise start with a promotable read only transaction.
	if opts.ReadOnly {
		c.nonPromotable = true
	}

	c.tx, err = c.db.Begin(false)
	return c, err
}

func (c *conn) Commit() error {
	err := c.tx.Commit()
	c.tx = nil
	c.nonPromotable = false
	return err
}

func (c *conn) Rollback() error {
	err := c.tx.Rollback()
	c.tx = nil
	c.nonPromotable = false
	return err
}

// Stmt is a prepared statement. It is bound to a Conn and not
// used by multiple goroutines concurrently.
type stmt struct {
	db            *DB
	tx            *Tx
	q             query.Query
	nonPromotable bool
}

// NumInput returns the number of placeholder parameters.
func (s stmt) NumInput() int { return -1 }

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
func (s stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("not implemented")
}

// CheckNamedValue has the same behaviour as driver.DefaultParamaterConverter, except that
// it allows document.Documents to be passed as parameters.
// It implements the driver.NamedValueChecker interface.
func (s stmt) CheckNamedValue(nv *driver.NamedValue) error {
	if _, ok := nv.Value.(document.Document); ok {
		return nil
	}

	if _, ok := nv.Value.(document.Scanner); ok {
		return nil
	}

	var err error
	nv.Value, err = driver.DefaultParameterConverter.ConvertValue(nv.Value)
	return err
}

// ExecContext executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
func (s stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	var res *query.Result
	var err error

	// if calling ExecContext within a transaction, use it,
	// otherwise use DB.
	if s.tx != nil {
		res, err = s.q.Exec(s.tx.Transaction, args, s.nonPromotable)
	} else {
		res, err = s.q.Run(s.db.db, args)
	}

	if err != nil {
		return nil, err
	}

	// s.q.Run might return a stream if the last Statement is a Select,
	// make sure the result is closed before returning so any transaction
	// created by s.q.Run is closed.
	return res, res.Close()
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

	var res *query.Result
	var err error

	// if calling QueryContext within a transaction, use it,
	// otherwise use DB.
	if s.tx != nil {
		res, err = s.q.Exec(s.tx.Transaction, args, s.nonPromotable)
	} else {
		res, err = s.q.Run(s.db.db, args)
	}

	if err != nil {
		return nil, err
	}

	rs := newRecordStream(res)
	if len(s.q.Statements) == 0 {
		return rs, nil
	}

	lastStmt := s.q.Statements[len(s.q.Statements)-1]

	slct, ok := lastStmt.(query.SelectStmt)
	if ok && len(slct.Selectors) > 0 {
		rs.fields = make([]string, len(slct.Selectors))
		for i := range slct.Selectors {
			rs.fields[i] = slct.Selectors[i].Name()
		}
	}

	return rs, nil
}

// Close does nothing.
func (s stmt) Close() error {
	return nil
}

var errStop = errors.New("stop")

type recordStream struct {
	res      *query.Result
	cancelFn func()
	c        chan rec
	wg       sync.WaitGroup
	fields   []string
}

type rec struct {
	r   query.RecordMask
	err error
}

func newRecordStream(res *query.Result) *recordStream {
	ctx, cancel := context.WithCancel(context.Background())

	records := recordStream{
		res:      res,
		cancelFn: cancel,
		c:        make(chan rec),
	}
	records.wg.Add(1)

	go records.iterate(ctx)

	return &records
}

func (rs *recordStream) iterate(ctx context.Context) {
	defer rs.wg.Done()
	defer close(rs.c)

	select {
	case <-ctx.Done():
		return
	case <-rs.c:
	}

	err := rs.res.Iterate(func(r document.Document) error {
		select {
		case <-ctx.Done():
			return errStop
		case rs.c <- rec{
			r: r.(query.RecordMask),
		}:

			select {
			case <-ctx.Done():
				return errStop
			case <-rs.c:
				return nil
			}
		}

	})

	if err == errStop || err == nil {
		return
	}
	if err != nil {
		rs.c <- rec{
			err: err,
		}
		return
	}
}

// Columns returns the fields selected by the SELECT statement.
// If the wildcard was used, it returns one column named "record".
func (rs *recordStream) Columns() []string {
	return rs.fields
}

// Close closes the rows iterator.
func (rs *recordStream) Close() error {
	rs.cancelFn()
	return rs.res.Close()
}

func (rs *recordStream) Next(dest []driver.Value) error {
	rs.c <- rec{}

	rec, ok := <-rs.c
	if !ok {
		return io.EOF
	}

	if rec.err != nil {
		return rec.err
	}

	for i := range rs.fields {
		if rs.fields[i] == "*" {
			dest[i] = rec.r

			continue
		}

		f, err := rec.r.GetByField(rs.fields[i])
		if err != nil {
			return err
		}

		dest[i], err = f.Decode()
		if err != nil {
			return err
		}
	}

	return nil
}
