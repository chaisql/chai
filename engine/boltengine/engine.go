// Package boltengine implements a BoltDB engine.
package boltengine

import (
	"context"
	"encoding/binary"
	"os"

	"github.com/genjidb/genji/engine"
	bolt "go.etcd.io/bbolt"
)

const (
	separator byte = 0x1F
	// name of the bucket used to mark keys for deletion
	binBucket = "__bin"
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
func (e *Engine) Begin(ctx context.Context, opts engine.TxOptions) (engine.Transaction, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	tx, err := e.DB.Begin(opts.Writable)
	if err != nil {
		return nil, err
	}

	return &Transaction{
		ctx:      ctx,
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
	ctx      context.Context
	tx       *bolt.Tx
	writable bool
	// if set to true,
	// the __bin bucket will be cleanup on commit.
	cleanupBin bool
}

// Rollback the transaction. Can be used safely after commit.
func (t *Transaction) Rollback() error {
	err := t.tx.Rollback()
	if err == bolt.ErrTxClosed {
		return engine.ErrTransactionDiscarded
	}
	if err != nil {
		return err
	}

	select {
	case <-t.ctx.Done():
		return t.ctx.Err()
	default:
	}

	return nil
}

// Commit the transaction.
func (t *Transaction) Commit() error {
	select {
	case <-t.ctx.Done():
		_ = t.Rollback()
		return t.ctx.Err()
	default:
	}

	// remove keys marked for deletion
	if t.cleanupBin {
		err := t.cleanupBinBucket()
		if err != nil {
			return err
		}
	}

	err := t.tx.Commit()
	if err == bolt.ErrTxClosed {
		return engine.ErrTransactionDiscarded
	}
	return err
}

// GetStore returns a store by name. The store uses a Bolt bucket.
func (t *Transaction) GetStore(name []byte) (engine.Store, error) {
	select {
	case <-t.ctx.Done():
		return nil, t.ctx.Err()
	default:
	}

	b := t.tx.Bucket(name)
	if b == nil {
		return nil, engine.ErrStoreNotFound
	}

	return &Store{
		bucket: b,
		tx:     t.tx,
		ngTx:   t,
		name:   name,
		ctx:    t.ctx,
	}, nil
}

// CreateStore creates a bolt bucket and returns a store.
// If the store already exists, returns engine.ErrStoreAlreadyExists.
func (t *Transaction) CreateStore(name []byte) error {
	select {
	case <-t.ctx.Done():
		return t.ctx.Err()
	default:
	}

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
func (t *Transaction) DropStore(name []byte) error {
	select {
	case <-t.ctx.Done():
		return t.ctx.Err()
	default:
	}

	if !t.writable {
		return engine.ErrTransactionReadOnly
	}

	err := t.tx.DeleteBucket(name)
	if err == bolt.ErrBucketNotFound {
		return engine.ErrStoreNotFound
	}

	return err
}

func (t *Transaction) markForDeletion(bucketName, key []byte) error {
	// create the bin bucket
	bb, err := t.tx.CreateBucketIfNotExists([]byte(binBucket))
	if err != nil {
		return err
	}

	// store the key in the bin bucket.
	// store the offset of the key in the value.
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(bucketName)))
	err = bb.Put(append(bucketName, key...), buf[:n])
	if err != nil {
		return err
	}

	// tell the transaction to cleanup on commit
	t.cleanupBin = true
	return nil
}

func (t *Transaction) cleanupBinBucket() error {
	buckets := make(map[string]*bolt.Bucket)

	c := t.tx.Bucket([]byte(binBucket)).Cursor()
	for k, v := c.Seek(nil); k != nil; k, v = c.Next() {
		offset, _ := binary.Uvarint(v)
		bucketName, key := k[:int(offset)], k[int(offset):]
		b, ok := buckets[string(bucketName)]
		if !ok {
			b = t.tx.Bucket(bucketName)
			// if b is nil, the bucket must have been deleted during this transaction
			// after having deleted some of its keys, we can ignore it.
			if b == nil {
				continue
			}

			buckets[string(bucketName)] = b
		}

		// if the key has been rewritten during the lifecycle of the transaction
		// do not delete it
		if b.Get(key) == nil {
			err := b.Delete(key)
			if err != nil {
				return err
			}
		}
	}

	// we can now drop the bin bucket
	return t.tx.DeleteBucket([]byte(binBucket))
}
