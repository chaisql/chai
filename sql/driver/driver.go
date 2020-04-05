package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync"

	"github.com/asdine/genji"
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/sql/parser"
	"github.com/asdine/genji/sql/query"
)

func init() {
	sql.Register("genji", sqlDriver{})
}

// sqlDriver is a driver.Driver that can open a new connection to a Genji database.
// It is the driver used to register Genji against the database/sql package.
type sqlDriver struct{}

func (d sqlDriver) Open(name string) (driver.Conn, error) {
	db, err := genji.Open(name)
	if err != nil {
		return nil, err
	}

	return &conn{db: db}, nil
}

// proxyDriver is used to turn an existing DB into a driver.Driver.
type proxyDriver struct {
	db *genji.DB
}

func newDriver(db *genji.DB) driver.Driver {
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

func newProxyConnector(db *genji.DB) driver.Connector {
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
	db            *genji.DB
	tx            *genji.Tx
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
	db            *genji.DB
	tx            *genji.Tx
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

// CheckNamedValue has the same behaviour as driver.DefaultParameterConverter, except that
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

	var res *query.Result
	var err error

	// if calling ExecContext within a transaction, use it,
	// otherwise use DB.
	if s.tx != nil {
		res, err = s.q.Exec(s.tx.Transaction, driverNamedValueToParams(args), s.nonPromotable)
	} else {
		res, err = s.q.Run(s.db.DB, driverNamedValueToParams(args))
	}

	if err != nil {
		return nil, err
	}

	// s.q.Run might return a stream if the last Statement is a Select,
	// make sure the result is closed before returning so any transaction
	// created by s.q.Run is closed.
	return result{res}, res.Close()
}

type result struct {
	*query.Result
}

// LastInsertId is not supported and returns an error.
// Use LastInsertKey instead.
func (r result) LastInsertId() (int64, error) {
	return driver.RowsAffected(r.Result.RowsAffected).LastInsertId()
}

// RowsAffected returns the number of rows affected by the
// query.
func (r result) RowsAffected() (int64, error) {
	return driver.RowsAffected(r.Result.RowsAffected).RowsAffected()
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
		res, err = s.q.Exec(s.tx.Transaction, driverNamedValueToParams(args), s.nonPromotable)
	} else {
		res, err = s.q.Run(s.db.DB, driverNamedValueToParams(args))
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

func driverNamedValueToParams(args []driver.NamedValue) []query.Param {
	params := make([]query.Param, len(args))
	for i, arg := range args {
		params[i].Name = arg.Name
		params[i].Value = arg.Value
	}

	return params
}

// Close does nothing.
func (s stmt) Close() error {
	return nil
}

var errStop = errors.New("stop")

type documentStream struct {
	res      *query.Result
	cancelFn func()
	c        chan doc
	wg       sync.WaitGroup
	fields   []string
}

type doc struct {
	d   document.Document
	err error
}

func newRecordStream(res *query.Result) *documentStream {
	ctx, cancel := context.WithCancel(context.Background())

	ds := documentStream{
		res:      res,
		cancelFn: cancel,
		c:        make(chan doc),
	}
	ds.wg.Add(1)

	go ds.iterate(ctx)

	return &ds
}

func (rs *documentStream) iterate(ctx context.Context) {
	defer rs.wg.Done()
	defer close(rs.c)

	select {
	case <-ctx.Done():
		return
	case <-rs.c:
	}

	err := rs.res.Iterate(func(d document.Document) error {
		select {
		case <-ctx.Done():
			return errStop
		case rs.c <- doc{
			d: d,
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
		rs.c <- doc{
			err: err,
		}
		return
	}
}

// Columns returns the fields selected by the SELECT statement.
func (rs *documentStream) Columns() []string {
	return rs.fields
}

// Close closes the rows iterator.
func (rs *documentStream) Close() error {
	rs.cancelFn()
	return rs.res.Close()
}

func (rs *documentStream) Next(dest []driver.Value) error {
	rs.c <- doc{}

	doc, ok := <-rs.c
	if !ok {
		return io.EOF
	}

	if doc.err != nil {
		return doc.err
	}

	for i := range rs.fields {
		if rs.fields[i] == "*" {
			dest[i] = doc.d

			continue
		}

		f, err := doc.d.GetByField(rs.fields[i])
		if err != nil {
			return err
		}

		dest[i] = f.V
	}

	return nil
}

type valueScanner struct {
	v interface{}
}

func (v valueScanner) Scan(src interface{}) error {
	switch t := src.(type) {
	case document.Document:
		return document.StructScan(t, v.v)
	case document.Array:
		return document.SliceScan(t, v.v)
	case document.Value:
		return document.ScanValue(t, src)
	}

	vv, err := document.NewValue(src)
	if err != nil {
		return err
	}

	return document.ScanValue(vv, &src)
}

// Scanner turns a variable into a sql.Scanner.
// x must be a pointer to a valid variable.
func Scanner(x interface{}) sql.Scanner {
	return valueScanner{x}
}
