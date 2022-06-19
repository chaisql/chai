// Package database provides database primitives such as tables, transactions and indexes.
package database

import (
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/genjidb/genji/internal/encoding"
	"github.com/genjidb/genji/internal/kv"
)

const (
	InternalPrefix = "__genji_"
)

type Database struct {
	DB      *pebble.DB
	Catalog *Catalog

	// If this is non-nil, the user is running an explicit transaction
	// using the BEGIN statement.
	// Only one attached transaction can be run at a time and any calls to DB.Begin()
	// will cause an error until that transaction is rolled back or commited.
	attachedTransaction *Transaction
	attachedTxMu        sync.Mutex

	// This limits the number of write transactions to 1.
	writetxmu *sync.Mutex

	// TransactionIDs is used to assign transaction an ID at runtime.
	// Since transaction IDs are not persisted and not used for concurrent
	// access, we can use 8 bytes ids that will be reset every time
	// the database restarts.
	TransactionIDs uint64

	closeOnce sync.Once

	// Underlying kv store.
	Store *kv.Store
}

// TxOptions are passed to Begin to configure transactions.
type TxOptions struct {
	// Open a read-only transaction.
	ReadOnly bool
	// Set the transaction as global at the database level.
	// Any queries run by the database will use that transaction until it is
	// rolled back or commited.
	Attached bool
}

// New initializes the DB using the given engine.
func New(pdb *pebble.DB) (*Database, error) {
	db := Database{
		DB:        pdb,
		Catalog:   NewCatalog(),
		writetxmu: &sync.Mutex{},
		Store: kv.NewStore(pdb, kv.Options{
			RollbackSegmentNamespace: int64(RollbackSegmentNamespace),
		}),
	}

	// ensure the rollback segment doesn't contain any data that needs to be rolled back
	// due to a previous crash.
	err := db.Store.Rollback()
	if err != nil {
		return nil, err
	}

	tx, err := db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	err = db.cleanupTransientNamespaces(tx)
	if err != nil {
		return nil, err
	}

	err = db.Catalog.Init(tx)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return &db, nil
}

// Close the database.
func (db *Database) Close() error {
	var err error

	db.closeOnce.Do(func() {
		err = db.closeDatabase()
	})

	return err
}

func (db *Database) closeDatabase() error {
	// If there is an attached transaction
	// it must be rolled back before closing the engine.
	if tx := db.GetAttachedTx(); tx != nil {
		_ = tx.Rollback()
	}
	db.writetxmu.Lock()
	defer db.writetxmu.Unlock()

	// release all sequences
	tx, err := db.beginTx(nil)
	if err != nil {
		return err
	}
	defer tx.Session.Close()

	for _, seqName := range db.Catalog.ListSequences() {
		seq, err := db.Catalog.GetSequence(seqName)
		if err != nil {
			return err
		}

		err = seq.Release(tx, db.Catalog)
		if err != nil {
			return err
		}
	}

	return tx.Session.Commit()
}

// GetAttachedTx returns the transaction attached to the database. It returns nil if there is no
// such transaction.
// The returned transaction is not thread safe.
func (db *Database) GetAttachedTx() *Transaction {
	db.attachedTxMu.Lock()
	defer db.attachedTxMu.Unlock()

	return db.attachedTransaction
}

// Begin starts a new transaction with default options.
// The returned transaction must be closed either by calling Rollback or Commit.
func (db *Database) Begin(writable bool) (*Transaction, error) {
	return db.BeginTx(&TxOptions{
		ReadOnly: !writable,
	})
}

// BeginTx starts a new transaction with the given options.
// If opts is empty, it will use the default options.
// The returned transaction must be closed either by calling Rollback or Commit.
// If the Attached option is passed, it opens a database level transaction, which gets
// attached to the database and prevents any other transaction to be opened afterwards
// until it gets rolled back or commited.
func (db *Database) BeginTx(opts *TxOptions) (*Transaction, error) {
	if opts == nil {
		opts = new(TxOptions)
	}

	if !opts.ReadOnly {
		db.writetxmu.Lock()
	}

	db.attachedTxMu.Lock()
	defer db.attachedTxMu.Unlock()

	if db.attachedTransaction != nil {
		return nil, errors.New("cannot open a transaction within a transaction")
	}

	return db.beginTx(opts)
}

// beginTx creates a transaction without locks.
func (db *Database) beginTx(opts *TxOptions) (*Transaction, error) {
	if opts == nil {
		opts = &TxOptions{}
	}

	var sess kv.Session
	if opts.ReadOnly {
		sess = db.Store.NewSnapshotSession()
	} else {
		sess = db.Store.NewBatchSession()
	}

	tx := Transaction{
		Store:    db.Store,
		Session:  sess,
		Writable: !opts.ReadOnly,
		ID:       atomic.AddUint64(&db.TransactionIDs, 1),
	}

	if !opts.ReadOnly {
		tx.WriteTxMu = db.writetxmu
	}

	if opts.Attached {
		db.attachedTransaction = &tx
		tx.OnRollbackHooks = append(tx.OnRollbackHooks, db.releaseAttachedTx)
		tx.OnCommitHooks = append(tx.OnCommitHooks, db.releaseAttachedTx)
	}

	return &tx, nil
}

func (db *Database) releaseAttachedTx() {
	db.attachedTxMu.Lock()
	defer db.attachedTxMu.Unlock()

	if db.attachedTransaction != nil {
		db.attachedTransaction = nil
	}
}

// ensures the transient namespaces are all empty before starting the database.
func (db *Database) cleanupTransientNamespaces(tx *Transaction) error {
	return tx.Session.DeleteRange(
		encoding.EncodeUint(nil, uint64(MinTransientNamespace)),
		encoding.EncodeUint(nil, uint64(MaxTransientNamespace)),
	)
}
