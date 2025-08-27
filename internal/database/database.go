// Package database provides database primitives such as tables, transactions and indexes.
package database

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chaisql/chai/internal/engine"
	"github.com/chaisql/chai/internal/kv"
	"github.com/cockroachdb/errors"
)

const (
	InternalPrefix = "__chai_"
)

type Database struct {
	catalogMu sync.RWMutex
	catalog   *Catalog

	// context used to notify all connections that the database is closing.
	closeContext context.Context
	closeCancel  context.CancelFunc

	// waitgroup to wait for all connections to be closed.
	connectionWg sync.WaitGroup

	// This is used to prevent creating a new transaction
	// during certain operations (commit, close, etc.)
	txmu sync.RWMutex

	// This limits the number of write transactions to 1.
	writetxmu sync.Mutex

	// transactionIDs is used to assign transaction an ID at runtime.
	// Since transaction IDs are not persisted and not used for concurrent
	// access, we can use 8 bytes ids that will be reset every time
	// the database restarts.
	transactionIDs atomic.Uint64

	closeOnce sync.Once

	// Underlying kv store.
	Engine engine.Engine
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
	LoadCatalog(engine.Session) (*Catalog, error)
}

// TxOptions are passed to Begin to configure transactions.
type TxOptions struct {
	// Open a read-only transaction.
	ReadOnly bool
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
		Engine: store,
	}

	// create a context that will be cancelled when the database is closed.
	db.closeContext, db.closeCancel = context.WithCancel(context.Background())

	// ensure the rollback segment doesn't contain any data that needs to be rolled back
	// due to a previous crash.
	err = db.Engine.Recover()
	if err != nil {
		return nil, err
	}

	// clean up the transient namespaces
	err = db.Engine.CleanupTransientNamespaces()
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
		db.closeCancel()

		db.connectionWg.Wait()
		err = db.closeDatabase()
	})

	return err
}

func (db *Database) closeDatabase() error {
	// release all sequences
	tx, err := db.beginTxUnlocked(nil)
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

	return db.Engine.Close()
}

// Connect returns a new connection to the database.
// The returned connection is not thread safe.
// It is the caller's responsibility to close the connection.
func (db *Database) Connect() (*Connection, error) {
	if db.closeContext.Err() != nil {
		return nil, errors.New("database is closed")
	}

	db.connectionWg.Add(1)
	return &Connection{
		db:  db,
		ctx: db.closeContext,
	}, nil
}

// Begin starts a new transaction with default options.
// The returned transaction must be closed either by calling Rollback or Commit.
func (db *Database) Begin(writable bool) (*Transaction, error) {
	if db.closeContext.Err() != nil {
		return nil, errors.New("database is closed")
	}

	return db.beginTx(&TxOptions{
		ReadOnly: !writable,
	})
}

// BeginTx starts a new transaction with the given options.
// If opts is empty, it will use the default options.
// The returned transaction must be closed either by calling Rollback or Commit.
func (db *Database) beginTx(opts *TxOptions) (*Transaction, error) {
	if db.closeContext.Err() != nil {
		return nil, errors.New("database is closed")
	}

	if opts == nil {
		opts = new(TxOptions)
	}

	if !opts.ReadOnly {
		db.writetxmu.Lock()
	}

	db.txmu.RLock()
	defer db.txmu.RUnlock()

	return db.beginTxUnlocked(opts)
}

// beginTxUnlocked creates a transaction without locks.
func (db *Database) beginTxUnlocked(opts *TxOptions) (*Transaction, error) {
	if opts == nil {
		opts = &TxOptions{}
	}

	var sess engine.Session
	if opts.ReadOnly {
		sess = db.Engine.NewSnapshotSession()
	} else {
		sess = db.Engine.NewBatchSession()
	}

	tx := Transaction{
		db:       db,
		Engine:   db.Engine,
		Session:  sess,
		Writable: !opts.ReadOnly,
		ID:       db.transactionIDs.Add(1),
		Catalog:  db.Catalog(),
		TxStart:  time.Now(),
	}

	if !opts.ReadOnly {
		tx.WriteTxMu = &db.writetxmu
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
