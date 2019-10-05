package database

import (
	"github.com/asdine/genji/engine"
	"github.com/pkg/errors"
)

// Tx represents a database transaction. It provides methods for managing the
// collection of tables and the transaction itself.
// Tx is either read-only or read/write. Read-only can be used to read tables
// and read/write can be used to read, create, delete and modify tables.
type Tx struct {
	db       *DB
	tx       engine.Transaction
	writable bool
}

// Rollback the transaction. Can be used safely after commit.
func (tx *Tx) Rollback() error {
	return tx.tx.Rollback()
}

// Commit the transaction.
func (tx *Tx) Commit() error {
	return tx.tx.Commit()
}

// Writable indicates if the transaction is writable or not.
func (tx *Tx) Writable() bool {
	return tx.writable
}

// Promote rollsback a read-only transaction and begins a read-write transaction transparently.
// It returns an error if the current transaction is already writable.
func (tx *Tx) Promote() error {
	if tx.writable {
		return errors.New("can't promote a writable transaction")
	}

	err := tx.Rollback()
	if err != nil {
		return err
	}

	newTx, err := tx.db.Begin(true)
	if err != nil {
		return err
	}

	tx.tx = newTx.tx
	tx.writable = newTx.writable
	return nil
}

// CreateTable creates a table with the given name.
// If it already exists, returns ErrTableAlreadyExists.
func (tx Tx) CreateTable(name string) (*Table, error) {
	err := tx.tx.CreateStore(name)
	if err == engine.ErrStoreAlreadyExists {
		return nil, ErrTableAlreadyExists
	}

	if err != nil {
		return nil, errors.Wrapf(err, "failed to create table %q", name)
	}

	s, err := tx.tx.Store(name)
	return &Table{
		tx:    &tx,
		store: s,
		name:  name,
	}, nil
}

// CreateTableIfNotExists calls CreateTable and returns no error if it already exists.
func (tx Tx) CreateTableIfNotExists(name string) (*Table, error) {
	t, err := tx.CreateTable(name)
	if err == nil {
		return t, nil
	}

	if err == ErrTableAlreadyExists {
		return tx.GetTable(name)
	}

	return nil, err
}

// GetTable returns a table by name. The table instance is only valid for the lifetime of the transaction.
func (tx Tx) GetTable(name string) (*Table, error) {
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
