// Package database provides database primitives such as tables, transactions and indexes.
package database

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/genjidb/genji/document/encoding"
	"github.com/genjidb/genji/engine"
)

// A Database manages a list of tables in an engine.
type Database struct {
	ng engine.Engine

	// tableInfoStore manages information about all the tables
	tableInfoStore *tableInfoStore

	// This stores the last transaction id created.
	// It starts at 0 at database startup and is
	// incremented atomically every time Begin is called.
	lastTransactionID int64

	// If this is non-nil, the user is running an explicit transaction
	// using the BEGIN statement.
	// Only one attached transaction can be run at a time and any calls to DB.Begin()
	// will cause an error until that transaction is rolled back or commited.
	attachedTransaction *Transaction
	attachedTxMu        sync.Mutex

	// Codec used to encode documents. Defaults to MessagePack.
	Codec encoding.Codec
}

type Options struct {
	Codec encoding.Codec
}

// New initializes the DB using the given engine.
func New(ctx context.Context, ng engine.Engine, opts Options) (*Database, error) {
	if opts.Codec == nil {
		return nil, errors.New("missing codec")
	}

	db := Database{
		ng:    ng,
		Codec: opts.Codec,
	}

	ntx, err := db.ng.Begin(ctx, engine.TxOptions{
		Writable: true,
	})
	if err != nil {
		return nil, err
	}
	defer ntx.Rollback()

	err = db.initInternalStores(ntx)
	if err != nil {
		return nil, err
	}

	db.tableInfoStore, err = newTableInfoStore(&db, ntx)
	if err != nil {
		return nil, err
	}

	err = ntx.Commit()
	if err != nil {
		return nil, err
	}

	return &db, nil
}

func (db *Database) initInternalStores(tx engine.Transaction) error {
	_, err := tx.GetStore([]byte(tableInfoStoreName))
	if err == engine.ErrStoreNotFound {
		err = tx.CreateStore([]byte(tableInfoStoreName))
	}
	if err != nil {
		return err
	}

	_, err = tx.GetStore([]byte(indexStoreName))
	if err == engine.ErrStoreNotFound {
		err = tx.CreateStore([]byte(indexStoreName))
	}
	return err
}

// Close the underlying engine.
func (db *Database) Close() error {
	return db.ng.Close()
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

	db.attachedTxMu.Lock()
	defer db.attachedTxMu.Unlock()

	if db.attachedTransaction != nil {
		return nil, errors.New("cannot open a transaction within a transaction")
	}

	ntx, err := db.ng.Begin(ctx, engine.TxOptions{
		Writable: !opts.ReadOnly,
	})
	if err != nil {
		return nil, err
	}

	tx := Transaction{
		id:             atomic.AddInt64(&db.lastTransactionID, 1),
		db:             db,
		tx:             ntx,
		writable:       !opts.ReadOnly,
		attached:       opts.Attached,
		tableInfoStore: db.tableInfoStore,
	}

	tx.indexStore, err = tx.getIndexStore()
	if err != nil {
		return nil, err
	}

	if opts.Attached {
		db.attachedTransaction = &tx
	}

	return &tx, nil
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

// GetAttachedTx returns the transaction attached to the database. It returns nil if there is no
// such transaction.
// The returned transaction is not thread safe.
func (db *Database) GetAttachedTx() *Transaction {
	db.attachedTxMu.Lock()
	defer db.attachedTxMu.Unlock()

	return db.attachedTransaction
}
