package genji

import (
	"context"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/database/catalogstore"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/query"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/types"
)

// DB represents a collection of tables stored in the underlying engine.
type DB struct {
	DB  *database.Database
	ctx context.Context
}

func newDatabase(ctx context.Context, ng engine.Engine, opts database.Options) (*DB, error) {
	db, err := database.New(ctx, ng, opts)
	if err != nil {
		return nil, err
	}

	tx, err := db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	err = catalogstore.LoadCatalog(tx, db.Catalog)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return &DB{
		DB:  db,
		ctx: ctx,
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
	tx, err := db.DB.BeginTx(db.ctx, &database.TxOptions{
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
func (db *DB) Query(q string, args ...interface{}) (*Result, error) {
	stmt, err := db.Prepare(q)
	if err != nil {
		return nil, err
	}

	return stmt.Query(args...)
}

// QueryDocument runs the query and returns the first document.
// If the query returns no error, QueryDocument returns errs.ErrDocumentNotFound.
func (db *DB) QueryDocument(q string, args ...interface{}) (types.Document, error) {
	stmt, err := db.Prepare(q)
	if err != nil {
		return nil, err
	}

	return stmt.QueryDocument(args...)
}

// Exec a query against the database without returning the result.
func (db *DB) Exec(q string, args ...interface{}) error {
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
func (tx *Tx) Query(q string, args ...interface{}) (*Result, error) {
	stmt, err := tx.Prepare(q)
	if err != nil {
		return nil, err
	}

	return stmt.Query(args...)
}

// QueryDocument runs the query and returns the first document.
// If the query returns no error, QueryDocument returns errs.ErrDocumentNotFound.
func (tx *Tx) QueryDocument(q string, args ...interface{}) (types.Document, error) {
	stmt, err := tx.Prepare(q)
	if err != nil {
		return nil, err
	}

	return stmt.QueryDocument(args...)
}

// Exec a query against the database within tx and without returning the result.
func (tx *Tx) Exec(q string, args ...interface{}) (err error) {
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
func (s *Statement) Query(args ...interface{}) (*Result, error) {
	var r *statement.Result
	var err error

	r, err = s.pq.Run(newQueryContext(s.db, s.tx, argsToParams(args)))
	if err != nil {
		return nil, err
	}

	return &Result{result: r}, nil
}

// QueryDocument runs the query and returns the first document.
// If the query returns no error, QueryDocument returns errs.ErrDocumentNotFound.
func (s *Statement) QueryDocument(args ...interface{}) (d types.Document, err error) {
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

	return scanDocument(res)
}

func scanDocument(iter document.Iterator) (types.Document, error) {
	var d types.Document
	err := iter.Iterate(func(doc types.Document) error {
		d = doc
		return stream.ErrStreamClosed
	})
	if err != nil {
		return nil, err
	}

	if d == nil {
		return nil, errors.Wrap(errs.ErrDocumentNotFound)
	}

	fb := document.NewFieldBuffer()
	err = fb.Copy(d)
	return fb, err
}

// Exec a query against the database without returning the result.
func (s *Statement) Exec(args ...interface{}) (err error) {
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

	return res.Iterate(func(d types.Document) error {
		return nil
	})
}

// Result of a query.
type Result struct {
	result *statement.Result
}

func (r *Result) Iterate(fn func(d types.Document) error) error {
	return r.result.Iterate(fn)
}

func (r *Result) Fields() []string {
	if r.result.Iterator == nil {
		return nil
	}

	stmt, ok := r.result.Iterator.(*statement.StreamStmtIterator)
	if !ok || stmt.Stream.Op == nil {
		return nil
	}

	// Search for the ProjectOperator. If found, extract the projected expression list
	for op := stmt.Stream.First(); op != nil; op = op.GetNext() {
		if po, ok := op.(*stream.ProjectOperator); ok {
			// if there are no projected expression, it's a wildcard
			if len(po.Exprs) == 0 {
				break
			}

			fields := make([]string, len(po.Exprs))
			for i := range po.Exprs {
				fields[i] = stringutil.Sprintf("%s", po.Exprs[i])
			}

			return fields
		}
	}

	// the stream will output documents in a single field
	return []string{"*"}
}

// Close the result stream.
func (r *Result) Close() (err error) {
	if r == nil {
		return nil
	}

	return r.result.Close()
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
