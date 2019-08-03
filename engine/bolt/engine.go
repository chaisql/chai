// Package bolt implements a BoltDB engine.
package bolt

import (
	"bytes"
	"os"

	"github.com/asdine/genji/engine"
	bolt "github.com/etcd-io/bbolt"
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
func (e *Engine) Begin(writable bool) (engine.Transaction, error) {
	tx, err := e.DB.Begin(writable)
	if err != nil {
		return nil, err
	}

	return &Transaction{
		tx:       tx,
		writable: writable,
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

// Store returns a store by name. The store uses a Bolt bucket.
func (t *Transaction) Store(name string) (engine.Store, error) {
	bname := []byte(name)
	b := t.tx.Bucket(bname)
	if b == nil {
		return nil, engine.ErrStoreNotFound
	}

	return &Store{
		bucket: b,
		tx:     t.tx,
		name:   bname,
	}, nil
}

// CreateStore creates a bolt bucket and returns a store.
// If the store already exists, returns engine.ErrStoreAlreadyExists.
func (t *Transaction) CreateStore(name string) error {
	if !t.writable {
		return engine.ErrTransactionReadOnly
	}

	_, err := t.tx.CreateBucket([]byte(name))
	if err == bolt.ErrBucketExists {
		return engine.ErrStoreAlreadyExists
	}

	return err
}

// DropStore deletes the underlying bucket.
func (t *Transaction) DropStore(name string) error {
	if !t.writable {
		return engine.ErrTransactionReadOnly
	}

	err := t.tx.DeleteBucket([]byte(name))
	if err == bolt.ErrBucketNotFound {
		return engine.ErrStoreNotFound
	}

	return err
}

// ListStores returns a list of all the store names.
func (t *Transaction) ListStores(prefix string) ([]string, error) {
	var names []string
	p := []byte(prefix)
	err := t.tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
		if bytes.HasPrefix(name, p) {
			names = append(names, string(name))
		}
		return nil
	})

	return names, err
}
