package genji

import (
	"context"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/query"
	"github.com/genjidb/genji/sql/parser"
	"github.com/genjidb/genji/stream"
)

// DB represents a collection of tables stored in the underlying engine.
type DB struct {
	DB  *database.Database
	ctx context.Context
}

// WithContext creates a new database handle using the given context for every operation.
func (db *DB) WithContext(ctx context.Context) *DB {
	return &DB{
		DB:  db.DB,
		ctx: ctx,
	}
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
		Transaction: tx,
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
func (db *DB) Query(q string, args ...interface{}) (*query.Result, error) {
	stmt, err := db.Prepare(q)
	if err != nil {
		return nil, err
	}

	return stmt.Query(args...)
}

// QueryDocument runs the query and returns the first document.
// If the query returns no error, QueryDocument returns database.ErrDocumentNotFound.
func (db *DB) QueryDocument(q string, args ...interface{}) (document.Document, error) {
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
	*database.Transaction
}

// Query the database withing the transaction and returns the result.
// Closing the returned result after usage is not mandatory.
func (tx *Tx) Query(q string, args ...interface{}) (*query.Result, error) {
	stmt, err := tx.Prepare(q)
	if err != nil {
		return nil, err
	}

	return stmt.Query(args...)
}

// QueryDocument runs the query and returns the first document.
// If the query returns no error, QueryDocument returns database.ErrDocumentNotFound.
func (tx *Tx) QueryDocument(q string, args ...interface{}) (document.Document, error) {
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

	return &Statement{
		pq: pq,
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
func (s *Statement) Query(args ...interface{}) (*query.Result, error) {
	if s.tx != nil {
		return s.pq.Exec(s.tx.Transaction, argsToParams(args))
	}

	return s.pq.Run(s.db.ctx, s.db.DB, argsToParams(args))
}

// QueryDocument runs the query and returns the first document.
// If the query returns no error, QueryDocument returns database.ErrDocumentNotFound.
func (s *Statement) QueryDocument(args ...interface{}) (d document.Document, err error) {
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

func scanDocument(res *query.Result) (document.Document, error) {
	var d document.Document
	err := res.Iterate(func(doc document.Document) error {
		d = doc
		return stream.ErrStreamClosed
	})
	if err != nil {
		return nil, err
	}

	if d == nil {
		return nil, database.ErrDocumentNotFound
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

	return res.Iterate(func(d document.Document) error {
		return nil
	})
}
