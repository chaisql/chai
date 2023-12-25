// Package database provides database primitives such as tables, transactions and indexes.
package database

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/chaisql/chai/internal/kv"
	"github.com/cockroachdb/errors"
)

const (
	InternalPrefix = "__chai_"
)

type Database struct {
	catalogMu sync.RWMutex
	catalog   *Catalog

	// If this is non-nil, the user is running an explicit transaction
	// using the BEGIN statement.
	// Only one attached transaction can be run at a time and any calls to DB.Begin()
	// will cause an error until that transaction is rolled back or commited.
	attachedTransaction *Transaction
	attachedTxMu        sync.Mutex

	// This is used to prevent creating a new transaction
	// during certain operations (commit, close, etc.)
	txmu sync.RWMutex

	// This limits the number of write transactions to 1.
	writetxmu sync.Mutex

	// TransactionIDs is used to assign transaction an ID at runtime.
	// Since transaction IDs are not persisted and not used for concurrent
	// access, we can use 8 bytes ids that will be reset every time
	// the database restarts.
	TransactionIDs uint64

	closeOnce sync.Once

	// Underlying kv store.
	Store *kv.Store
}

// Options are passed to Open to control
// how the database is loaded.
type Options struct {
	CatalogLoader func(tx *Transaction) error
}

// CatalogLoader loads the catalog from the disk.
// It may parse a SQL representation of the catalog
// and return a Catalog that represents all entities stored on disk.
type CatalogLoader interface {
	LoadCatalog(kv.Session) (*Catalog, error)
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

func Open(path string, opts *Options) (*Database, error) {
	store, err := kv.NewEngine(path, kv.Options{
		RollbackSegmentNamespace: int64(RollbackSegmentNamespace),
		MinTransientNamespace:    uint64(MinTransientNamespace),
		MaxTransientNamespace:    uint64(MaxTransientNamespace),
	})
	if err != nil {
		return nil, err
	}

	db := Database{
		Store: store,
	}

	// ensure the rollback segment doesn't contain any data that needs to be rolled back
	// due to a previous crash.
	err = db.Store.ResetRollbackSegment()
	if err != nil {
		return nil, err
	}

	// clean up the transient namespaces
	err = db.Store.CleanupTransientNamespaces()
	if err != nil {
		return nil, err
	}

	tx, err := db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	db.catalog = NewCatalog()
	tx.Catalog = db.catalog

	if opts.CatalogLoader != nil {
		err = opts.CatalogLoader(tx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to load catalog")
		}
	} else {
		err = tx.CatalogWriter().Init(tx)
		if err != nil {
			return nil, err
		}
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

	for _, seqName := range tx.Catalog.ListSequences() {
		seq, err := tx.Catalog.GetSequence(seqName)
		if err != nil {
			return err
		}

		err = seq.Release(tx)
		if err != nil {
			return err
		}
	}

	err = tx.Session.Commit()
	if err != nil {
		return err
	}

	return db.Store.Close()
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
	db.txmu.RLock()
	defer db.txmu.RUnlock()

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
		db:       db,
		Store:    db.Store,
		Session:  sess,
		Writable: !opts.ReadOnly,
		ID:       atomic.AddUint64(&db.TransactionIDs, 1),
		Catalog:  db.Catalog(),
		TxStart:  time.Now(),
	}

	if !opts.ReadOnly {
		tx.WriteTxMu = &db.writetxmu
	}

	if opts.Attached {
		db.attachedTransaction = &tx
		tx.OnRollbackHooks = append(tx.OnRollbackHooks, db.releaseAttachedTx)
		tx.OnCommitHooks = append(tx.OnCommitHooks, db.releaseAttachedTx)
	}

	return &tx, nil
}

func (db *Database) Catalog() *Catalog {
	db.catalogMu.RLock()
	c := db.catalog
	db.catalogMu.RUnlock()
	return c
}

func (db *Database) SetCatalog(c *Catalog) {
	db.catalogMu.Lock()
	db.catalog = c
	db.catalogMu.Unlock()
}

func (db *Database) releaseAttachedTx() {
	db.attachedTxMu.Lock()
	defer db.attachedTxMu.Unlock()

	if db.attachedTransaction != nil {
		db.attachedTransaction = nil
	}
}
