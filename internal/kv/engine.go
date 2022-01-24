// package kv implements a Pebble kv.
package kv

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

const (
	separator   byte = 0x1F
	storeKey         = "__genji.store"
	storePrefix      = 's'
)

// Common errors returned by the engine implementations.
var (
	// ErrTransactionReadOnly is returned when attempting to call write methods on a read-only transaction.
	ErrTransactionReadOnly = errors.New("transaction is read-only")

	// ErrTransactionDiscarded is returned when calling Rollback or Commit after a transaction is no longer valid.
	ErrTransactionDiscarded = errors.New("transaction has been discarded")

	// ErrStoreNotFound is returned when the targeted store doesn't exist.
	ErrStoreNotFound = errors.New("store not found")

	// ErrStoreAlreadyExists must be returned when attempting to create a store with the
	// same name as an existing one.
	ErrStoreAlreadyExists = errors.New("store already exists")

	// ErrKeyNotFound is returned when the targeted key doesn't exist.
	ErrKeyNotFound = errors.New("key not found")
)

// Engine represents a Pebble kv.
type Engine struct {
	DB   *pebble.DB
	opts *pebble.Options
}

// NewEngine creates a Pebble kv engine. It takes the same argument as Pebble's Open function.
func NewEngine(path string, opts *pebble.Options) (*Engine, error) {
	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, err
	}

	return &Engine{
		DB:   db,
		opts: opts,
	}, nil
}

// TxOptions is used to configure a transaction upon creation.
type TxOptions struct {
	Writable bool
}

// Begin creates a transaction using Pebble's batch API.
func (e *Engine) Begin(ctx context.Context, opts TxOptions) (*Transaction, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	var batch *pebble.Batch

	if opts.Writable {
		batch = e.DB.NewIndexedBatch()
	}

	return &Transaction{
		ctx:      ctx,
		ng:       e,
		batch:    batch,
		writable: opts.Writable,
	}, nil
}

func (e *Engine) NewTransientStore(ctx context.Context) (*TransientStore, error) {
	// build engine with fast options

	var inMemory bool
	if e.opts != nil {
		_, inMemory = e.opts.FS.(*vfs.MemFS)
	}

	opt := pebble.Options{
		DisableWAL: true,
	}

	var path string
	if inMemory {
		opt.FS = vfs.NewMem()
	} else {
		path = filepath.Join(os.TempDir(), fmt.Sprintf(".genji-transient-%d", time.Now().Unix()+rand.Int63()))

	}
	opt.Logger = nil

	db, err := pebble.Open(path, &opt)
	if err != nil {
		return nil, err
	}

	s := TransientStore{
		DB:    db,
		Path:  path,
		batch: db.NewIndexedBatch(),
	}

	err = s.Reset()
	if err != nil {
		return nil, err
	}

	return &s, nil
}

// Close the engine and underlying Pebble database.
func (e *Engine) Close() error {
	return e.DB.Close()
}

// A Transaction uses Pebble's batches.
type Transaction struct {
	ctx       context.Context
	ng        *Engine
	batch     *pebble.Batch
	writable  bool
	discarded bool
}

// Rollback the transaction. Can be used safely after commit.
func (t *Transaction) Rollback() error {
	if t.writable {
		_ = t.batch.Close()
	}

	if t.discarded {
		return errors.WithStack(ErrTransactionDiscarded)
	}

	t.discarded = true

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

	if t.discarded {
		return errors.WithStack(ErrTransactionDiscarded)
	}

	if !t.writable {
		return ErrTransactionReadOnly
	}

	t.discarded = true

	defer t.batch.Close()

	return t.batch.Commit(&pebble.WriteOptions{Sync: true})
}

func buildStoreKey(name []byte) []byte {
	var buf bytes.Buffer
	buf.Grow(len(storeKey) + 1 + len(name))
	buf.WriteString(storeKey)
	buf.WriteByte(separator)
	buf.Write(name)

	return buf.Bytes()
}

func buildStorePrefixKey(name []byte) []byte {
	prefix := make([]byte, 0, len(name)+3)
	prefix = append(prefix, storePrefix)
	prefix = append(prefix, separator)
	prefix = append(prefix, name...)

	return prefix
}

// GetStore returns a store by name.
func (t *Transaction) GetStore(name []byte) (*Store, error) {
	select {
	case <-t.ctx.Done():
		return nil, t.ctx.Err()
	default:
	}

	key := buildStoreKey(name)

	var closer io.Closer
	var err error
	if t.writable {
		_, closer, err = t.batch.Get(key)
	} else {
		_, closer, err = t.ng.DB.Get(key)
	}
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, errors.WithStack(ErrStoreNotFound)
		}

		return nil, err
	}
	err = closer.Close()
	if err != nil {
		return nil, err
	}

	pkey := buildStorePrefixKey(name)

	return &Store{
		ctx:      t.ctx,
		ng:       t.ng,
		tx:       t,
		Prefix:   pkey,
		writable: t.writable,
		name:     name,
	}, nil
}

// CreateStore creates a store.
// If the store already exists, returns ErrStoreAlreadyExists.
func (t *Transaction) CreateStore(name []byte) error {
	select {
	case <-t.ctx.Done():
		return t.ctx.Err()
	default:
	}

	if !t.writable {
		return errors.WithStack(ErrTransactionReadOnly)
	}

	key := buildStoreKey(name)
	_, closer, err := t.batch.Get(key)
	if err == nil {
		_ = closer.Close()
		return errors.WithStack(ErrStoreAlreadyExists)
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}

	return t.batch.Set(key, nil, nil)
}

// DropStore deletes the store and all its keys.
func (t *Transaction) DropStore(name []byte) error {
	select {
	case <-t.ctx.Done():
		return t.ctx.Err()
	default:
	}

	if !t.writable {
		return errors.WithStack(ErrTransactionReadOnly)
	}

	s, err := t.GetStore(name)
	if err != nil {
		return err
	}

	err = s.Truncate()
	if err != nil {
		return err
	}

	err = t.batch.Delete(buildStoreKey([]byte(name)), nil)
	if errors.Is(err, pebble.ErrNotFound) {
		return errors.WithStack(ErrStoreNotFound)
	}

	return err
}
