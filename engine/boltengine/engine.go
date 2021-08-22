// Package boltengine implements a BoltDB engine.
package boltengine

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/internal/errors"
	bolt "go.etcd.io/bbolt"
)

const (
	// name of the bucket used to mark keys for deletion
	binBucket = "__bin"
)

// Engine represents a BoltDB engine. Each store is stored in a dedicated bucket.
type Engine struct {
	DB *bolt.DB

	transient bool
}

// NewEngine creates a BoltDB engine. It takes the same argument as Bolt's Open function.
func NewEngine(path string, mode os.FileMode, opts *bolt.Options) (*Engine, error) {
	db, err := bolt.Open(path, mode, opts)
	if err != nil {
		return nil, errors.Wrap(err)
	}

	return &Engine{
		DB: db,
	}, nil
}

// Begin creates a transaction using Bolt's transaction API.
func (e *Engine) Begin(ctx context.Context, opts engine.TxOptions) (engine.Transaction, error) {
	select {
	case <-ctx.Done():
		return nil, errors.Wrap(ctx.Err())
	default:
	}

	tx, err := e.DB.Begin(opts.Writable)
	if err != nil {
		return nil, errors.Wrap(err)
	}

	return &Transaction{
		ctx:      ctx,
		tx:       tx,
		writable: opts.Writable,
	}, nil
}

func (e *Engine) NewTransientEngine(ctx context.Context) (engine.Engine, error) {
	// build engine with fast options
	ng, err := NewEngine(filepath.Join(os.TempDir(), fmt.Sprintf(".genji-transient-%d.db", time.Now().UnixNano()+rand.Int63())), 0600, &bolt.Options{
		NoFreelistSync: true,
		FreelistType:   bolt.FreelistMapType,
		NoSync:         true,
	})
	if err != nil {
		return nil, err
	}

	ng.transient = true
	return ng, nil
}

func (e *Engine) Drop(ctx context.Context) error {
	if !e.transient {
		return errors.New("cannot drop persistent engine")
	}

	p := e.DB.Path()

	_ = e.Close()

	return os.Remove(p)
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
	if errors.Is(err, bolt.ErrTxClosed) {
		return errors.Wrap(engine.ErrTransactionDiscarded)
	}
	if err != nil {
		return errors.Wrap(err)
	}

	select {
	case <-t.ctx.Done():
		return errors.Wrap(t.ctx.Err())
	default:
	}

	return nil
}

// Commit the transaction.
func (t *Transaction) Commit() error {
	select {
	case <-t.ctx.Done():
		_ = t.Rollback()
		return errors.Wrap(t.ctx.Err())
	default:
	}

	// remove keys marked for deletion
	if t.cleanupBin {
		err := t.cleanupBinBucket()
		if err != nil {
			return errors.Wrap(err)
		}
	}

	err := t.tx.Commit()
	if errors.Is(err, bolt.ErrTxClosed) {
		return errors.Wrap(engine.ErrTransactionDiscarded)
	}
	return errors.Wrap(err)
}

// GetStore returns a store by name. The store uses a Bolt bucket.
func (t *Transaction) GetStore(name []byte) (engine.Store, error) {
	select {
	case <-t.ctx.Done():
		return nil, errors.Wrap(t.ctx.Err())
	default:
	}

	b := t.tx.Bucket(name)
	if b == nil {
		return nil, errors.Wrap(engine.ErrStoreNotFound)
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
		return errors.Wrap(t.ctx.Err())
	default:
	}

	if !t.writable {
		return errors.Wrap(engine.ErrTransactionReadOnly)
	}

	_, err := t.tx.CreateBucket(name)
	if errors.Is(err, bolt.ErrBucketExists) {
		return errors.Wrap(engine.ErrStoreAlreadyExists)
	}

	return errors.Wrap(err)
}

// DropStore deletes the underlying bucket.
func (t *Transaction) DropStore(name []byte) error {
	select {
	case <-t.ctx.Done():
		return errors.Wrap(t.ctx.Err())
	default:
	}

	if !t.writable {
		return errors.Wrap(engine.ErrTransactionReadOnly)
	}

	err := t.tx.DeleteBucket(name)
	if errors.Is(err, bolt.ErrBucketNotFound) {
		return errors.Wrap(engine.ErrStoreNotFound)
	}

	return errors.Wrap(err)
}

func (t *Transaction) markForDeletion(bucketName, key []byte) error {
	// create the bin bucket
	bb, err := t.tx.CreateBucketIfNotExists([]byte(binBucket))
	if err != nil {
		return errors.Wrap(err)
	}

	// store the key in the bin bucket.
	// store the offset of the key in the value.
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(bucketName)))
	err = bb.Put(append(bucketName, key...), buf[:n])
	if err != nil {
		return errors.Wrap(err)
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
				return errors.Wrap(err)
			}
		}
	}

	// we can now drop the bin bucket
	return errors.Wrap(t.tx.DeleteBucket([]byte(binBucket)))
}
