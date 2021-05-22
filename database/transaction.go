package database

import (
	"github.com/genjidb/genji/engine"
)

// Transaction represents a database transaction. It provides methods for managing the
// collection of tables and the transaction itself.
// Transaction is either read-only or read/write. Read-only can be used to read tables
// and read/write can be used to read, create, delete and modify tables.
type Transaction struct {
	DB       *Database
	Tx       engine.Transaction
	Catalog  *Catalog
	Writable bool
	// if set to true, this transaction is attached to the database
	attached bool

	// these functions are run after a successful rollback.
	OnRollbackHooks []func()
}

// Rollback the transaction. Can be used safely after commit.
func (tx *Transaction) Rollback() error {
	err := tx.Tx.Rollback()
	if err != nil {
		return err
	}

	defer func() {
		if tx.Writable {
			tx.DB.txmu.Unlock()
		} else {
			tx.DB.txmu.RUnlock()
		}
	}()

	if tx.attached {
		tx.DB.attachedTxMu.Lock()
		defer tx.DB.attachedTxMu.Unlock()

		if tx.DB.attachedTransaction != nil {
			tx.DB.attachedTransaction = nil
		}
	}

	for i := len(tx.OnRollbackHooks) - 1; i >= 0; i-- {
		tx.OnRollbackHooks[i]()
	}

	return nil
}

// Commit the transaction. Calling this method on read-only transactions
// will return an error.
func (tx *Transaction) Commit() error {
	err := tx.Tx.Commit()
	if err != nil {
		return err
	}

	defer func() {
		if tx.Writable {
			tx.DB.txmu.Unlock()
		} else {
			tx.DB.txmu.RUnlock()
		}
	}()

	if tx.attached {
		tx.DB.attachedTxMu.Lock()
		defer tx.DB.attachedTxMu.Unlock()

		if tx.DB.attachedTransaction != nil {
			tx.DB.attachedTransaction = nil
		}
	}

	return nil
}
