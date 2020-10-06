// Package boltengine implements a BoltDB engine.
package boltengine

import (
	"context"
	"os"

	"github.com/genjidb/genji/engine"
	bolt "go.etcd.io/bbolt"
)

const (
	separator byte = 0x1F
)

// Engine represents a BoltDB engine. Each store is stored in a dedicated bucket.
type Engine struct {
	DB *bolt.DB
}

// NewEngine creates a BoltDB engine. It takes the same argument as Bolt's Open function.
func NewEngine(path string, mode os.FileMode, opts *bolt.Options) (*Engine, error) {
	db, err := bolt.Open(path, mode, opts)
	if err != nil {
		return nil, err
	}

	return &Engine{
		DB: db,
	}, nil
}

// Begin creates a transaction using Bolt's transaction API.
func (e *Engine) Begin(ctx context.Context, opts engine.TransactionOptions) (engine.Transaction, error) {
	tx, err := e.DB.Begin(opts.Writable)
	if err != nil {
		return nil, err
	}

	return &Transaction{
		tx:       tx,
		writable: opts.Writable,
	}, nil
}

// Close the engine and underlying Bolt database.
func (e *Engine) Close() error {
	return e.DB.Close()
}

// A Transaction uses Bolt's transactions.
type Transaction struct {
	tx       *bolt.Tx
	writable bool
}

// Rollback the transaction. Can be used safely after commit.
func (t *Transaction) Rollback() error {
	err := t.tx.Rollback()
	if err != nil && err != bolt.ErrTxClosed {
		return err
	}

	return nil
}

// Commit the transaction.
func (t *Transaction) Commit() error {
	return t.tx.Commit()
}

// GetStore returns a store by name. The store uses a Bolt bucket.
func (t *Transaction) GetStore(ctx context.Context, name []byte) (engine.Store, error) {
	b := t.tx.Bucket(name)
	if b == nil {
		return nil, engine.ErrStoreNotFound
	}

	return &Store{
		bucket: b,
		tx:     t.tx,
		name:   name,
	}, nil
}

// CreateStore creates a bolt bucket and returns a store.
// If the store already exists, returns engine.ErrStoreAlreadyExists.
func (t *Transaction) CreateStore(ctx context.Context, name []byte) error {
	if !t.writable {
		return engine.ErrTransactionReadOnly
	}

	_, err := t.tx.CreateBucket(name)
	if err == bolt.ErrBucketExists {
		return engine.ErrStoreAlreadyExists
	}

	return err
}

// DropStore deletes the underlying bucket.
func (t *Transaction) DropStore(ctx context.Context, name []byte) error {
	if !t.writable {
		return engine.ErrTransactionReadOnly
	}

	err := t.tx.DeleteBucket(name)
	if err == bolt.ErrBucketNotFound {
		return engine.ErrStoreNotFound
	}

	return err
}
