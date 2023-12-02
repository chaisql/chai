package database

import (
	"sync"

	"github.com/chaisql/chai/internal/kv"
	"github.com/cockroachdb/errors"
)

// Transaction represents a database transaction. It provides methods for managing the
// collection of tables and the transaction itself.
// Transaction is either read-only or read/write. Read-only can be used to read tables
// and read/write can be used to read, create, delete and modify tables.
type Transaction struct {
	db *Database

	Session   kv.Session
	Store     *kv.Store
	ID        uint64
	Writable  bool
	WriteTxMu *sync.Mutex
	// these functions are run after a successful rollback.
	OnRollbackHooks []func()
	// these functions are run after a successful commit.
	OnCommitHooks []func()

	Catalog       *Catalog
	catalogWriter *CatalogWriter
}

// Rollback the transaction. Can be used safely after commit.
func (tx *Transaction) Rollback() error {
	err := tx.Session.Close()
	if err != nil {
		return err
	}

	if tx.Writable {
		err = tx.Store.Rollback()
		if err != nil {
			return err
		}

		defer func() {
			tx.WriteTxMu.Unlock()
		}()
	}

	for i := len(tx.OnRollbackHooks) - 1; i >= 0; i-- {
		tx.OnRollbackHooks[i]()
	}

	return nil
}

// Commit the transaction. Calling this method on read-only transactions
// will return an error.
func (tx *Transaction) Commit() error {
	if !tx.Writable {
		return errors.New("cannot commit read-only transaction")
	}

	// lock the transaction mutex to prevent any other transaction
	// from being created while the commit is in progress.
	tx.db.txmu.Lock()
	defer tx.db.txmu.Unlock()

	err := tx.Session.Commit()
	if err != nil {
		return err
	}

	_ = tx.Session.Close()

	defer func() {
		tx.WriteTxMu.Unlock()
	}()

	for i := len(tx.OnCommitHooks) - 1; i >= 0; i-- {
		tx.OnCommitHooks[i]()
	}

	// if the catalog has been modified, update the database catalog
	if tx.catalogWriter != nil {
		tx.db.SetCatalog(tx.Catalog)
	}

	return nil
}

func (tx *Transaction) CatalogWriter() *CatalogWriter {
	if !tx.Writable {
		panic("cannot get catalog writer from read-only transaction")
	}

	if tx.catalogWriter == nil {
		tx.Catalog = tx.db.Catalog().Clone()
		// clone the catalog so that it can be modified without affecting the original one.
		tx.catalogWriter = NewCatalogWriter(tx.Catalog)
	}

	return tx.catalogWriter
}
