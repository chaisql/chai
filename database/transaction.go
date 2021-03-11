package database

import (
	"fmt"

	"github.com/genjidb/genji/engine"
)

var (
	internalPrefix     = "__genji_"
	tableInfoStoreName = internalPrefix + "tables"
	indexStoreName     = internalPrefix + "indexes"
)

// Transaction represents a database transaction. It provides methods for managing the
// collection of tables and the transaction itself.
// Transaction is either read-only or read/write. Read-only can be used to read tables
// and read/write can be used to read, create, delete and modify tables.
type Transaction struct {
	db       *Database
	tx       engine.Transaction
	writable bool
	// if set to true, this transaction is attached to the database
	attached bool

	// these functions are run after a successful rollback or commit.
	onRollbackHooks []func()
	onCommitHooks   []func()
}

// DB returns the underlying database that created the transaction.
func (tx *Transaction) DB() *Database {
	return tx.db
}

// Rollback the transaction. Can be used safely after commit.
func (tx *Transaction) Rollback() error {
	err := tx.tx.Rollback()
	if err != nil {
		return err
	}

	defer func() {
		if tx.writable {
			tx.db.txmu.Unlock()
		} else {
			tx.db.txmu.RUnlock()
		}
	}()

	if tx.attached {
		tx.db.attachedTxMu.Lock()
		defer tx.db.attachedTxMu.Unlock()

		if tx.db.attachedTransaction != nil {
			tx.db.attachedTransaction = nil
		}
	}

	for i := len(tx.onRollbackHooks) - 1; i >= 0; i-- {
		tx.onRollbackHooks[i]()
	}

	return nil
}

// Commit the transaction. Calling this method on read-only transactions
// will return an error.
func (tx *Transaction) Commit() error {
	err := tx.tx.Commit()
	if err != nil {
		return err
	}

	defer func() {
		if tx.writable {
			tx.db.txmu.Unlock()
		} else {
			tx.db.txmu.RUnlock()
		}
	}()

	if tx.attached {
		tx.db.attachedTxMu.Lock()
		defer tx.db.attachedTxMu.Unlock()

		if tx.db.attachedTransaction != nil {
			tx.db.attachedTransaction = nil
		}
	}

	for i := len(tx.onCommitHooks) - 1; i >= 0; i-- {
		tx.onCommitHooks[i]()
	}

	return nil

}

// Writable indicates if the transaction is writable or not.
func (tx *Transaction) Writable() bool {
	return tx.writable
}

// CreateTable creates a table with the given name.
// If it already exists, returns ErrTableAlreadyExists.
func (tx *Transaction) CreateTable(name string, info *TableInfo) error {
	return tx.db.catalog.CreateTable(tx, name, info)
}

// GetTable returns a table by name. The table instance is only valid for the lifetime of the transaction.
func (tx *Transaction) GetTable(name string) (*Table, error) {
	return tx.db.catalog.GetTable(tx, name)
}

// AddFieldConstraint adds a field constraint to a table.
func (tx *Transaction) AddFieldConstraint(tableName string, fc FieldConstraint) error {
	return tx.db.catalog.AddFieldConstraint(tx, tableName, fc)
}

// RenameTable renames a table.
// If it doesn't exist, it returns ErrTableNotFound.
func (tx *Transaction) RenameTable(oldName, newName string) error {
	return tx.db.catalog.RenameTable(tx, oldName, newName)
}

// DropTable deletes a table from the database.
func (tx *Transaction) DropTable(name string) error {
	return tx.db.catalog.DropTable(tx, name)
}

// CreateIndex creates an index with the given name.
// If it already exists, returns ErrIndexAlreadyExists.
func (tx *Transaction) CreateIndex(opts IndexInfo) error {
	return tx.db.catalog.CreateIndex(tx, opts)
}

// GetIndex returns an index by name.
func (tx *Transaction) GetIndex(name string) (*Index, error) {
	return tx.db.catalog.GetIndex(tx, name)
}

// DropIndex deletes an index from the database.
func (tx *Transaction) DropIndex(name string) error {
	return tx.db.catalog.DropIndex(tx, name)
}

// ListIndexes lists all indexes.
func (tx *Transaction) ListIndexes() []string {
	return tx.db.catalog.ListIndexes("")
}

// ReIndex truncates and recreates selected index from scratch.
func (tx *Transaction) ReIndex(indexName string) error {
	return tx.db.catalog.ReIndex(tx, indexName)
}

// ReIndexAll truncates and recreates all indexes of the database from scratch.
func (tx *Transaction) ReIndexAll() error {
	return tx.db.catalog.ReIndexAll(tx)
}

func (tx *Transaction) getTableStore() *tableStore {
	st, err := tx.tx.GetStore([]byte(tableInfoStoreName))
	if err != nil {
		panic(fmt.Sprintf("database incorrectly setup: missing %q table: %v", tableInfoStoreName, err))
	}

	return &tableStore{
		st: st,
		db: tx.db,
	}
}

func (tx *Transaction) getIndexStore() *indexStore {
	st, err := tx.tx.GetStore([]byte(indexStoreName))
	if err != nil {
		panic(fmt.Sprintf("database incorrectly setup: missing %q table: %v", indexStoreName, err))
	}

	return &indexStore{
		st: st,
		db: tx.db,
	}
}
