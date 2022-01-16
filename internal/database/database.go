// Package database provides database primitives such as tables, transactions and indexes.
package database

import (
	"context"
	"sync"

	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/kv"
)

const (
	InternalPrefix = "__genji_"
)

type Database struct {
	ng      *kv.Engine
	Catalog *Catalog

	// If this is non-nil, the user is running an explicit transaction
	// using the BEGIN statement.
	// Only one attached transaction can be run at a time and any calls to DB.Begin()
	// will cause an error until that transaction is rolled back or commited.
	attachedTransaction *Transaction
	attachedTxMu        sync.Mutex

	// This controls concurrency on read-only and read/write transactions.
	txmu *sync.RWMutex

	// Pool of reusable transient engines to use for temporary indices.
	TransientStorePool *TransientStorePool

	closeOnce sync.Once
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
func New(ctx context.Context, ng *kv.Engine) (*Database, error) {
	db := Database{
		ng:      ng,
		Catalog: NewCatalog(),
		txmu:    &sync.RWMutex{},
		TransientStorePool: &TransientStorePool{
			ng: ng,
		},
	}

	tx, err := db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

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

// NewTransientStore creates a temporary store to be used for creating temporary indices.
func (db *Database) NewTransientStore(ctx context.Context) (*kv.TransientStore, func() error, error) {
	tdb, err := db.TransientStorePool.Get(context.Background())
	if err != nil {
		return nil, nil, err
	}

	return tdb, func() error {
		return db.TransientStorePool.Release(context.Background(), tdb)
	}, nil
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
	db.txmu.Lock()
	defer db.txmu.Unlock()

	// release all sequences
	tx, err := db.beginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Tx.Rollback()

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

	return tx.Tx.Commit()
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
	return db.BeginTx(context.Background(), &TxOptions{
		ReadOnly: !writable,
	})
}

// BeginTx starts a new transaction with the given options.
// If opts is empty, it will use the default options.
// The returned transaction must be closed either by calling Rollback or Commit.
// If the Attached option is passed, it opens a database level transaction, which gets
// attached to the database and prevents any other transaction to be opened afterwards
// until it gets rolled back or commited.
func (db *Database) BeginTx(ctx context.Context, opts *TxOptions) (*Transaction, error) {
	if opts == nil {
		opts = new(TxOptions)
	}

	if !opts.ReadOnly {
		db.txmu.Lock()
	} else {
		db.txmu.RLock()
	}

	db.attachedTxMu.Lock()
	defer db.attachedTxMu.Unlock()

	if db.attachedTransaction != nil {
		return nil, errors.New("cannot open a transaction within a transaction")
	}

	return db.beginTx(ctx, opts)
}

// beginTx creates a transaction without locks.
func (db *Database) beginTx(ctx context.Context, opts *TxOptions) (*Transaction, error) {
	if opts == nil {
		opts = &TxOptions{}
	}

	ntx, err := db.ng.Begin(ctx, kv.TxOptions{
		Writable: !opts.ReadOnly,
	})
	if err != nil {
		return nil, err
	}

	tx := Transaction{
		Tx:       ntx,
		Writable: !opts.ReadOnly,
		DBMu:     db.txmu,
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
