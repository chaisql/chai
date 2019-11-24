package genji

import (
	"database/sql"
	"database/sql/driver"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/engine/boltengine"
	"github.com/asdine/genji/engine/memoryengine"
	"github.com/asdine/genji/parser"
	"github.com/asdine/genji/query"
	"github.com/asdine/genji/record"
)

// Open creates a Genji database at the given path.
// If path is equal to ":memory:" it will open an in memory database,
// otherwise it will create an on-disk database using the BoltDB engine.
func Open(path string) (*DB, error) {
	var ng engine.Engine
	var err error

	switch path {
	case ":memory:":
		ng = memoryengine.NewEngine()
	default:
		ng, err = boltengine.NewEngine(path, 0660, nil)
	}
	if err != nil {
		return nil, err
	}

	return New(ng)
}

// DB represents a collection of tables stored in the underlying engine.
type DB struct {
	db *database.Database
}

// New initializes the DB using the given engine.
func New(ng engine.Engine) (*DB, error) {
	db, err := database.New(ng)
	if err != nil {
		return nil, err
	}

	return &DB{
		db: db,
	}, nil
}

// Close the database.
func (db *DB) Close() error {
	return db.db.Close()
}

// Begin starts a new transaction.
// The returned transaction must be closed either by calling Rollback or Commit.
func (db *DB) Begin(writable bool) (*Tx, error) {
	tx, err := db.db.Begin(writable)
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

	return pq.Run(db.db, argsToNamedValues(args))
}

// QueryRecord runs the query and returns the first record.
// If the query returns no error, QueryRecord returns ErrRecordNotFound.
func (db *DB) QueryRecord(q string, args ...interface{}) (record.Record, error) {
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
		return nil, database.ErrRecordNotFound
	}

	var fb record.FieldBuffer
	err = fb.ScanRecord(r)
	if err != nil {
		return nil, err
	}

	return &fb, nil
}

// ViewTable starts a read only transaction, fetches the selected table, calls fn with that table
// and automatically rolls back the transaction.
func (db *DB) ViewTable(tableName string, fn func(*Tx, *database.Table) error) error {
	return db.View(func(tx *Tx) error {
		tb, err := tx.GetTable(tableName)
		if err != nil {
			return err
		}

		return fn(tx, tb)
	})
}

// UpdateTable starts a read/write transaction, fetches the selected table, calls fn with that table
// and automatically commits the transaction.
// If fn returns an error, the transaction is rolled back.
func (db *DB) UpdateTable(tableName string, fn func(*Tx, *database.Table) error) error {
	return db.Update(func(tx *Tx) error {
		tb, err := tx.GetTable(tableName)
		if err != nil {
			return err
		}

		return fn(tx, tb)
	})
}

// SQLDB returns a sql.DB wrapping this database.
func (db *DB) SQLDB() *sql.DB {
	return sql.OpenDB(newProxyConnector(db))
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

	return pq.Exec(tx.Transaction, argsToNamedValues(args), false)
}

// QueryRecord runs the query and returns the first record.
// If the query returns no error, QueryRecord returns ErrRecordNotFound.
func (tx *Tx) QueryRecord(q string, args ...interface{}) (record.Record, error) {
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
		return nil, database.ErrRecordNotFound
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

func argsToNamedValues(args []interface{}) []driver.NamedValue {
	nv := make([]driver.NamedValue, len(args))
	for i := range args {
		switch t := args[i].(type) {
		case sql.NamedArg:
			nv[i].Name = t.Name
			nv[i].Value = t.Value
		case *sql.NamedArg:
			nv[i].Name = t.Name
			nv[i].Value = t.Value
		case driver.NamedValue:
			nv[i] = t
		case *driver.NamedValue:
			nv[i] = *t
		default:
			nv[i].Ordinal = i + 1
			nv[i].Value = args[i]
		}
	}

	return nv
}
