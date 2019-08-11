package genji

import (
	"math/rand"
	"strings"
	"time"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/table"
	"github.com/pkg/errors"
)

var (
	entropy          = rand.New(rand.NewSource(time.Now().UnixNano()))
	separator   byte = 0x1F
	indexTable       = "__genji.indexes"
	indexPrefix      = "i"
)

var (
	// ErrTableNotFound is returned when the targeted table doesn't exist.
	ErrTableNotFound = errors.New("table not found")

	// ErrTableAlreadyExists is returned when attempting to create a table with the
	// same name as an existing one.
	ErrTableAlreadyExists = errors.New("table already exists")

	// ErrIndexNotFound is returned when the targeted index doesn't exist.
	ErrIndexNotFound = errors.New("index not found")

	// ErrIndexAlreadyExists is returned when attempting to create an index with the
	// same name as an existing one.
	ErrIndexAlreadyExists = errors.New("index already exists")
)

// DB represents a collection of tables stored in the underlying engine.
// DB differs from the engine in that it provides automatic indexing
// and database administration methods.
// DB is safe for concurrent use unless the given engine isn't.
type DB struct {
	ng engine.Engine
}

// New initializes the DB using the given engine.
func New(ng engine.Engine) (*DB, error) {
	db := DB{
		ng: ng,
	}

	err := db.Update(func(tx *Tx) error {
		return tx.CreateTableIfNotExists(indexTable)
	})
	if err != nil {
		return nil, err
	}

	return &db, nil
}

// Close the underlying engine.
func (db DB) Close() error {
	return db.ng.Close()
}

// Begin starts a new transaction.
// The returned transaction must be closed either by calling Rollback or Commit.
func (db DB) Begin(writable bool) (*Tx, error) {
	tx, err := db.ng.Begin(writable)
	if err != nil {
		return nil, err
	}

	return &Tx{
		tx: tx,
	}, nil
}

// View starts a read only transaction, runs fn and automatically rolls it back.
func (db DB) View(fn func(tx *Tx) error) error {
	tx, err := db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	return fn(tx)
}

// Update starts a read-write transaction, runs fn and automatically commits it.
func (db DB) Update(fn func(tx *Tx) error) error {
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

// ViewTable starts a read only transaction, fetches the selected table, calls fn with that table
// and automatically rolls back the transaction.
func (db DB) ViewTable(tableName string, fn func(*Table) error) error {
	return db.View(func(tx *Tx) error {
		tb, err := tx.Table(tableName)
		if err != nil {
			return err
		}

		return fn(tb)
	})
}

// UpdateTable starts a read/write transaction, fetches the selected table, calls fn with that table
// and automatically commits the transaction.
// If fn returns an error, the transaction is rolled back.
func (db DB) UpdateTable(tableName string, fn func(*Table) error) error {
	return db.Update(func(tx *Tx) error {
		tb, err := tx.Table(tableName)
		if err != nil {
			return err
		}

		return fn(tb)
	})
}

// Tx represents a database transaction. It provides methods for managing the
// collection of tables and the transaction itself.
// Tx is either read-only or read/write. Read-only can be used to read tables
// and read/write can be used to read, create, delete and modify tables.
type Tx struct {
	tx engine.Transaction
}

// Rollback the transaction. Can be used safely after commit.
func (tx Tx) Rollback() error {
	return tx.tx.Rollback()
}

// Commit the transaction.
func (tx Tx) Commit() error {
	return tx.tx.Commit()
}

// CreateTable creates a table with the given name.
// If it already exists, returns ErrTableAlreadyExists.
func (tx Tx) CreateTable(name string) error {
	err := tx.tx.CreateStore(name)
	if err == engine.ErrStoreAlreadyExists {
		return ErrTableAlreadyExists
	}
	return errors.Wrapf(err, "failed to create table %q", name)
}

// CreateTableIfNotExists calls CreateTable and returns no error if it already exists.
func (tx Tx) CreateTableIfNotExists(name string) error {
	err := tx.CreateTable(name)
	if err == nil || err == ErrTableAlreadyExists {
		return nil
	}
	return err
}

// Table returns a table by name. The table instance is only valid for the lifetime of the transaction.
func (tx Tx) Table(name string) (*Table, error) {
	s, err := tx.tx.Store(name)
	if err == engine.ErrStoreNotFound {
		return nil, ErrTableNotFound
	}
	if err != nil {
		return nil, err
	}

	return &Table{
		tx:    &tx,
		store: s,
		name:  name,
	}, nil
}

// DropTable deletes a table from the database.
func (tx Tx) DropTable(name string) error {
	err := tx.tx.DropStore(name)
	if err == engine.ErrStoreNotFound {
		return ErrTableNotFound
	}
	return err
}

func buildIndexName(tableName, field string) string {
	var b strings.Builder
	b.WriteString(indexPrefix)
	b.WriteString(tableName)
	b.WriteByte(separator)
	b.WriteString(field)

	return b.String()
}

// CreateIndex creates an index with the given name.
// If it already exists, returns ErrTableAlreadyExists.
func (tx Tx) CreateIndex(tableName, field string, opts index.Options) error {
	_, err := tx.Table(tableName)
	if err != nil {
		return err
	}

	it, err := tx.Table(indexTable)
	if err != nil {
		return err
	}

	idxName := buildIndexName(tableName, field)

	_, err = it.Record([]byte(idxName))
	if err == nil {
		return ErrIndexAlreadyExists
	}
	if err != table.ErrRecordNotFound {
		return err
	}

	_, err = it.Insert(&indexOptions{
		TableName: tableName,
		FieldName: field,
		Unique:    opts.Unique,
	})
	if err != nil {
		return err
	}

	err = tx.tx.CreateStore(idxName)
	return errors.Wrapf(err, "failed to create index %q on table %q", field, tableName)
}

// CreateIndexIfNotExists calls CreateIndex and returns no error if it already exists.
func (tx Tx) CreateIndexIfNotExists(table string, field string, opts index.Options) error {
	err := tx.CreateIndex(table, field, opts)
	if err == nil || err == ErrIndexAlreadyExists {
		return nil
	}
	return err
}

// Index returns an index by name.
func (tx Tx) Index(tableName, field string) (index.Index, error) {
	_, err := tx.Table(tableName)
	if err != nil {
		return nil, err
	}

	indexName := buildIndexName(tableName, field)

	opts, err := readIndexOptions(&tx, indexName)
	if err != nil {
		return nil, err
	}

	s, err := tx.tx.Store(buildIndexName(tableName, field))
	if err == engine.ErrStoreNotFound {
		return nil, ErrIndexNotFound
	}
	if err != nil {
		return nil, err
	}

	return index.New(s, index.Options{Unique: opts.Unique}), nil
}

// Indexes returns a map of all the indexes of a table.
func (tx Tx) Indexes(tableName string) (map[string]index.Index, error) {
	prefix := buildIndexName(tableName, "")
	list, err := tx.tx.ListStores(prefix)
	if err != nil {
		return nil, err
	}

	indexes := make(map[string]index.Index)
	for _, storeName := range list {
		idxName := strings.TrimPrefix(storeName, prefix)
		indexes[idxName], err = tx.Index(tableName, idxName)
		if err != nil {
			return nil, err
		}
	}

	return indexes, nil
}

// DropIndex deletes an index from the database.
func (tx Tx) DropIndex(tableName, field string) error {
	it, err := tx.Table(indexTable)
	if err != nil {
		return err
	}

	indexName := buildIndexName(tableName, field)
	err = it.Delete([]byte(indexName))
	if err == table.ErrRecordNotFound {
		return ErrIndexNotFound
	}
	if err != nil {
		return err
	}

	err = tx.tx.DropStore(indexName)
	if err == engine.ErrStoreNotFound {
		return ErrIndexNotFound
	}
	return err
}
