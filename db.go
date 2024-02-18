/*
package chai implements an embedded SQL database.
*/
package chai

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"io"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/database/catalogstore"
	"github.com/chaisql/chai/internal/environment"
	errs "github.com/chaisql/chai/internal/errors"
	"github.com/chaisql/chai/internal/query"
	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/rows"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

// DB represents a collection of tables.
type DB struct {
	DB  *database.Database
	ctx context.Context
}

// Open creates a Chai database at the given path.
// If path is equal to ":memory:" it will open an in-memory database,
// otherwise it will create an on-disk database.
func Open(path string) (*DB, error) {
	db, err := database.Open(path, &database.Options{
		CatalogLoader: catalogstore.LoadCatalog,
	})
	if err != nil {
		return nil, err
	}

	return &DB{
		DB: db,
	}, nil
}

func (db *DB) Connect() (*Connection, error) {
	conn, err := db.DB.Connect()
	if err != nil {
		return nil, err
	}

	return &Connection{
		db:   db,
		Conn: conn,
	}, nil
}

// WithContext creates a new database handle using the given context for every operation.
func (db DB) WithContext(ctx context.Context) *DB {
	db.ctx = ctx
	return &db
}

func (db *DB) withConn(fn func(*Connection) error) error {
	conn, err := db.Connect()
	if err != nil {
		return err
	}
	defer conn.Close()

	return fn(conn)
}

// QueryRow runs the query and returns the first row.
func (db *DB) QueryRow(q string, args ...any) (r *Row, err error) {
	err = db.withConn(func(c *Connection) error {
		r, err = c.QueryRow(q, args...)
		return err
	})
	return
}

// Exec a query against the database without returning the result.
func (db *DB) Exec(q string, args ...any) error {
	return db.withConn(func(c *Connection) error {
		return c.Exec(q, args...)
	})
}

// Close the database.
func (db *DB) Close() error {
	return db.DB.Close()
}

type Connection struct {
	db   *DB
	Conn *database.Connection
}

// Begin starts a new transaction.
// The returned transaction must be closed either by calling Rollback or Commit.
func (c *Connection) Begin(writable bool) (*Tx, error) {
	_, err := c.Conn.BeginTx(&database.TxOptions{
		ReadOnly: !writable,
	})
	if err != nil {
		return nil, err
	}

	return &Tx{
		conn: c,
	}, nil
}

// View starts a read only transaction, runs fn and automatically rolls it back.
func (c *Connection) View(fn func(tx *Tx) error) error {
	tx, err := c.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	return fn(tx)
}

// Update starts a read-write transaction, runs fn and automatically commits it.
func (c *Connection) Update(fn func(tx *Tx) error) error {
	tx, err := c.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = fn(tx)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// Query the database and return the result.
// The returned result must always be closed after usage.
func (c *Connection) Query(q string, args ...any) (*Result, error) {
	stmt, err := c.Prepare(q)
	if err != nil {
		return nil, err
	}

	res, err := stmt.Query(args...)
	if err != nil {
		return nil, err
	}

	res.conn = c

	return res, nil
}

// QueryRow runs the query and returns the first row.
func (c *Connection) QueryRow(q string, args ...any) (*Row, error) {
	stmt, err := c.Prepare(q)
	if err != nil {
		return nil, err
	}

	return stmt.QueryRow(args...)
}

// Exec a query against the database without returning the result.
func (c *Connection) Exec(q string, args ...any) error {
	stmt, err := c.Prepare(q)
	if err != nil {
		return err
	}

	return stmt.Exec(args...)
}

// Prepare parses the query and returns a prepared statement.
func (c *Connection) Prepare(q string) (*Statement, error) {
	pq, err := parser.ParseQuery(q)
	if err != nil {
		return nil, err
	}

	err = pq.Prepare(newQueryContext(c, nil))
	if err != nil {
		return nil, err
	}

	return &Statement{
		pq:   pq,
		conn: c,
	}, nil
}

func (c *Connection) Close() error {
	return c.Conn.Close()
}

// Tx represents a database transaction. It provides methods for managing the
// collection of tables and the transaction itself.
// Tx is either read-only or read/write. Read-only can be used to read tables
// and read/write can be used to read, create, delete and modify tables.
type Tx struct {
	conn *Connection
}

// Rollback the transaction. Can be used safely after commit.
func (tx *Tx) Rollback() error {
	t := tx.conn.Conn.GetTx()
	if t == nil {
		return errors.New("transaction has already been committed or rolled back")
	}

	return t.Rollback()
}

// Commit the transaction. Calling this method on read-only transactions
// will return an error.
func (tx *Tx) Commit() error {
	t := tx.conn.Conn.GetTx()
	if t == nil {
		return errors.New("transaction has already been committed or rolled back")
	}

	return t.Commit()
}

// Query the database withing the transaction and returns the result.
// Closing the returned result after usage is not mandatory.
func (tx *Tx) Query(q string, args ...any) (*Result, error) {
	stmt, err := tx.Prepare(q)
	if err != nil {
		return nil, err
	}

	return stmt.Query(args...)
}

// QueryRow runs the query and returns the first row.
func (tx *Tx) QueryRow(q string, args ...any) (*Row, error) {
	stmt, err := tx.Prepare(q)
	if err != nil {
		return nil, err
	}

	return stmt.QueryRow(args...)
}

// Exec a query against the database within tx and without returning the result.
func (tx *Tx) Exec(q string, args ...any) (err error) {
	stmt, err := tx.Prepare(q)
	if err != nil {
		return err
	}

	return stmt.Exec(args...)
}

// Prepare parses the query and returns a prepared statement.
func (tx *Tx) Prepare(q string) (*Statement, error) {
	pq, err := parser.ParseQuery(q)
	if err != nil {
		return nil, err
	}

	err = pq.Prepare(newQueryContext(tx.conn, nil))
	if err != nil {
		return nil, err
	}

	return &Statement{
		pq:   pq,
		conn: tx.conn,
		tx:   tx,
	}, nil
}

// Statement is a prepared statement. If Statement has been created on a Tx,
// it will only be valid until Tx closes. If it has been created on a DB, it
// is valid until the DB closes.
// It's safe for concurrent use by multiple goroutines.
type Statement struct {
	pq   query.Query
	conn *Connection
	tx   *Tx
}

// Query the database and return the result.
// The returned result must always be closed after usage.
func (s *Statement) Query(args ...any) (*Result, error) {
	var r *statement.Result
	var err error

	r, err = s.pq.Run(newQueryContext(s.conn, argsToParams(args)))
	if err != nil {
		return nil, err
	}

	return &Result{result: r, ctx: s.conn.db.ctx}, nil
}

func argsToParams(args []interface{}) []environment.Param {
	nv := make([]environment.Param, len(args))
	for i := range args {
		switch t := args[i].(type) {
		case sql.NamedArg:
			nv[i].Name = t.Name
			nv[i].Value = t.Value
		case *sql.NamedArg:
			nv[i].Name = t.Name
			nv[i].Value = t.Value
		case driver.NamedValue:
			nv[i].Name = t.Name
			nv[i].Value = t.Value
		case *driver.NamedValue:
			nv[i].Name = t.Name
			nv[i].Value = t.Value
		case *environment.Param:
			nv[i] = *t
		case environment.Param:
			nv[i] = t
		default:
			nv[i].Value = args[i]
		}
	}

	return nv
}

// QueryRow runs the query and returns the first row.
func (s *Statement) QueryRow(args ...any) (r *Row, err error) {
	res, err := s.Query(args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		er := res.Close()
		if err == nil {
			err = er
		}
	}()

	return res.GetFirst()
}

// Exec a query against the database without returning the result.
func (s *Statement) Exec(args ...any) (err error) {
	res, err := s.Query(args...)
	if err != nil {
		return err
	}
	defer func() {
		er := res.Close()
		if err == nil {
			err = er
		}
	}()

	return res.Iterate(func(*Row) error {
		return nil
	})
}

// Result of a query.
type Result struct {
	result *statement.Result
	ctx    context.Context
	conn   *Connection
}

func (r *Result) Iterate(fn func(r *Row) error) error {
	var row Row
	if r.ctx == nil {
		return r.result.Iterate(func(dr database.Row) error {
			row.row = dr
			return fn(&row)
		})
	}

	return r.result.Iterate(func(dr database.Row) error {
		if err := r.ctx.Err(); err != nil {
			return err
		}

		row.row = dr
		return fn(&row)
	})
}

func (r *Result) GetFirst() (*Row, error) {
	var rr *Row
	err := r.Iterate(func(row *Row) error {
		rr = row.Clone()
		return stream.ErrStreamClosed
	})
	if err != nil {
		return nil, err
	}

	if rr == nil {
		return nil, errors.WithStack(errs.NewRowNotFoundError())
	}

	return rr, nil
}

func (r *Result) Columns() []string {
	if r.result.Iterator == nil {
		return nil
	}

	stmt, ok := r.result.Iterator.(*statement.StreamStmtIterator)
	if !ok || stmt.Stream.Op == nil {
		return nil
	}

	// Search for the ProjectOperator. If found, extract the projected expression list
	for op := stmt.Stream.First(); op != nil; op = op.GetNext() {
		if po, ok := op.(*rows.ProjectOperator); ok {
			// if there are no projected expression, it's a wildcard
			if len(po.Exprs) == 0 {
				break
			}

			columns := make([]string, len(po.Exprs))
			for i := range po.Exprs {
				columns[i] = po.Exprs[i].String()
			}

			return columns
		}
	}

	// the stream will output rows in a single field
	return []string{"*"}
}

// Close the result stream.
func (r *Result) Close() (err error) {
	if r == nil {
		return nil
	}

	err = r.result.Close()

	return err
}

func (r *Result) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	err := r.MarshalJSONTo(&buf)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (r *Result) MarshalJSONTo(w io.Writer) error {
	buf := bufio.NewWriter(w)

	buf.WriteByte('[')

	first := true
	err := r.result.Iterate(func(r database.Row) error {
		if !first {
			buf.WriteString(", ")
		} else {
			first = false
		}

		data, err := r.MarshalJSON()
		if err != nil {
			return err
		}

		_, err = buf.Write(data)
		return err
	})
	if err != nil {
		return err
	}

	buf.WriteByte(']')
	return buf.Flush()
}

func newQueryContext(conn *Connection, params []environment.Param) *query.Context {
	return &query.Context{
		Ctx:    conn.db.ctx,
		DB:     conn.db.DB,
		Conn:   conn.Conn,
		Params: params,
	}
}

type Row struct {
	row database.Row
}

func (r *Row) Clone() *Row {
	var rr Row
	cb := row.NewColumnBuffer()
	err := cb.Copy(r.row)
	if err != nil {
		panic(err)
	}
	var br database.BasicRow
	br.ResetWith(r.row.TableName(), r.row.Key(), cb)
	rr.row = &br

	return &rr
}

func (r *Row) Columns() ([]string, error) {
	var cols []string
	err := r.row.Iterate(func(column string, value types.Value) error {
		cols = append(cols, column)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return cols, nil
}
func (r *Row) GetColumnType(column string) (string, error) {
	v, err := r.row.Get(column)
	if errors.Is(err, types.ErrColumnNotFound) {
		return "", err
	}

	return v.Type().String(), err
}

func (r *Row) ScanColumn(column string, dest any) error {
	return row.ScanColumn(r.row, column, dest)
}

func (r *Row) Scan(dest ...any) error {
	return row.Scan(r.row, dest...)
}

func (r *Row) StructScan(dest any) error {
	return row.StructScan(r.row, dest)
}

func (r *Row) MapScan(dest map[string]any) error {
	return row.MapScan(r.row, dest)
}

func (r *Row) MarshalJSON() ([]byte, error) {
	return r.row.MarshalJSON()
}
