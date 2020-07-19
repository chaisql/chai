package genji

import (
	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/sql/parser"
	"github.com/genjidb/genji/sql/query"
)

// DB represents a collection of tables stored in the underlying engine.
type DB struct {
	DB *database.Database
}

// New initializes the DB using the given engine.
func New(ng engine.Engine) (*DB, error) {
	db, err := database.New(ng)
	if err != nil {
		return nil, err
	}

	return &DB{
		DB: db,
	}, nil
}

// Close the database.
func (db *DB) Close() error {
	return db.DB.Close()
}

// Begin starts a new transaction.
// The returned transaction must be closed either by calling Rollback or Commit.
func (db *DB) Begin(writable bool) (*Tx, error) {
	tx, err := db.DB.Begin(writable)
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

// Exec a query against the database without returning the result.
func (db *DB) Exec(q string, args ...interface{}) error {
	res, err := db.Query(q, args...)
	if err != nil {
		return err
	}

	return res.Close()
}

// Query the database and return the result.
// The returned result must always be closed after usage.
func (db *DB) Query(q string, args ...interface{}) (*query.Result, error) {
	pq, err := parser.ParseQuery(q)
	if err != nil {
		return nil, err
	}

	return pq.Run(db.DB, argsToParams(args))
}

// QueryDocument runs the query and returns the first document.
// If the query returns no error, QueryDocument returns database.ErrDocumentNotFound.
func (db *DB) QueryDocument(q string, args ...interface{}) (document.Document, error) {
	res, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	r, err := res.First()
	if err != nil {
		return nil, err
	}

	if r == nil {
		return nil, database.ErrDocumentNotFound
	}

	var fb document.FieldBuffer
	err = fb.ScanDocument(r)
	if err != nil {
		return nil, err
	}

	return &fb, nil
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
	pq, err := parser.ParseQuery(q)
	if err != nil {
		return nil, err
	}

	return pq.Exec(tx.Transaction, argsToParams(args), false)
}

// QueryDocument runs the query and returns the first document.
// If the query returns no error, QueryDocument returns database.ErrDocumentNotFound.
func (tx *Tx) QueryDocument(q string, args ...interface{}) (document.Document, error) {
	res, err := tx.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	r, err := res.First()
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, database.ErrDocumentNotFound
	}

	return r, nil
}

// Exec a query against the database within tx and without returning the result.
func (tx *Tx) Exec(q string, args ...interface{}) error {
	res, err := tx.Query(q, args...)
	if err != nil {
		return err
	}

	return res.Close()
}
