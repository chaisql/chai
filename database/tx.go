package database

import (
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/record"
	"github.com/pkg/errors"
)

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

type indexer interface {
	Indexes() map[string]index.Options
}

// InitTable ensures the table exists before returning it.
// If r implements the following interface, its method will be called to
// call CreateIndexesIfNotExist and create all missing indexes.
//   type indexer interface {
//	   Indexes() map[string]index.Options
//   }
// Note that if a an index is created, the table won't be reindexed. Use the
// ReIndex method to do so.
func (tx Tx) InitTable(name string, r record.Record) (*Table, error) {
	t, err := tx.CreateTableIfNotExists(name)
	if err != nil {
		return nil, err
	}

	if idxer, ok := r.(indexer); ok {
		err = t.CreateIndexesIfNotExist(idxer.Indexes())
		if err != nil {
			return nil, err
		}
	}

	return t, nil
}
