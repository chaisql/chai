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

// WithContext creates a new database handle using the given context for every operation.
func (db DB) WithContext(ctx context.Context) *DB {
	db.ctx = ctx
	return &db
}

// Close the database.
func (db *DB) Close() error {
	return db.DB.Close()
}

// Begin starts a new transaction.
// The returned transaction must be closed either by calling Rollback or Commit.
func (db *DB) Begin(writable bool) (*Tx, error) {
	tx, err := db.DB.BeginTx(&database.TxOptions{
		ReadOnly: !writable,
	})
	if err != nil {
		return nil, err
	}

	return &Tx{
		db: db,
		tx: tx,
	}, nil
}

// View starts a read only transaction, runs fn and automatically rolls it back.
func (db *DB) View(fn func(tx *Tx) error) error {
	tx, err := db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	return fn(tx)
}

// Update starts a read-write transaction, runs fn and automatically commits it.
func (db *DB) Update(fn func(tx *Tx) error) error {
	tx, err := db.Begin(true)
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
func (db *DB) Query(q string, args ...any) (*Result, error) {
	stmt, err := db.Prepare(q)
	if err != nil {
		return nil, err
	}

	return stmt.Query(args...)
}

// QueryRow runs the query and returns the first row.
func (db *DB) QueryRow(q string, args ...any) (*Row, error) {
	stmt, err := db.Prepare(q)
	if err != nil {
		return nil, err
	}

	return stmt.QueryRow(args...)
}

// Exec a query against the database without returning the result.
func (db *DB) Exec(q string, args ...any) error {
	stmt, err := db.Prepare(q)
	if err != nil {
		return err
	}

	return stmt.Exec(args...)
}

// Prepare parses the query and returns a prepared statement.
func (db *DB) Prepare(q string) (*Statement, error) {
	pq, err := parser.ParseQuery(q)
	if err != nil {
		return nil, err
	}

	err = pq.Prepare(newQueryContext(db, nil, nil))
	if err != nil {
		return nil, err
	}

	return &Statement{
		pq: pq,
		db: db,
	}, nil
}

// Tx represents a database transaction. It provides methods for managing the
// collection of tables and the transaction itself.
// Tx is either read-only or read/write. Read-only can be used to read tables
// and read/write can be used to read, create, delete and modify tables.
type Tx struct {
	db *DB
	tx *database.Transaction
}

// Rollback the transaction. Can be used safely after commit.
func (tx *Tx) Rollback() error {
	return tx.tx.Rollback()
}

// Commit the transaction. Calling this method on read-only transactions
// will return an error.
func (tx *Tx) Commit() error {
	return tx.tx.Commit()
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

	err = pq.Prepare(newQueryContext(tx.db, tx, nil))
	if err != nil {
		return nil, err
	}

	return &Statement{
		pq: pq,
		db: tx.db,
		tx: tx,
	}, nil
}

// Statement is a prepared statement. If Statement has been created on a Tx,
// it will only be valid until Tx closes. If it has been created on a DB, it
// is valid until the DB closes.
// It's safe for concurrent use by multiple goroutines.
type Statement struct {
	pq query.Query
	db *DB
	tx *Tx
}

// Query the database and return the result.
// The returned result must always be closed after usage.
func (s *Statement) Query(args ...any) (*Result, error) {
	var r *statement.Result
	var err error

	r, err = s.pq.Run(newQueryContext(s.db, s.tx, argsToParams(args)))
	if err != nil {
		return nil, err
	}

	return &Result{result: r, ctx: s.db.ctx}, nil
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

			fields := make([]string, len(po.Exprs))
			for i := range po.Exprs {
				fields[i] = po.Exprs[i].String()
			}

			return fields
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

	return r.result.Close()
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

func newQueryContext(db *DB, tx *Tx, params []environment.Param) *query.Context {
	ctx := query.Context{
		Ctx:    db.ctx,
		DB:     db.DB,
		Params: params,
	}

	if tx != nil {
		ctx.Tx = tx.tx
	}

	return &ctx
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
