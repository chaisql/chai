// package kv implements a Badger kv.
package kv

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
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

// Engine represents a Badger kv.
type Engine struct {
	DB *badger.DB
}

// NewEngine creates a Badger kv. It takes the same argument as Badger's Open function.
func NewEngine(opt badger.Options) (*Engine, error) {
	db, err := badger.Open(opt)
	if err != nil {
		return nil, err
	}

	return &Engine{
		DB: db,
	}, nil
}

// TxOptions is used to configure a transaction upon creation.
type TxOptions struct {
	Writable bool
}

// Begin creates a transaction using Badger's transaction API.
func (e *Engine) Begin(ctx context.Context, opts TxOptions) (*Transaction, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	tx := e.DB.NewTransaction(opts.Writable)

	return &Transaction{
		ctx:      ctx,
		ng:       e,
		tx:       tx,
		writable: opts.Writable,
	}, nil
}

func (e *Engine) NewTransientStore(ctx context.Context) (*TransientStore, error) {
	// build engine with fast options

	inMemory := e.DB.Opts().InMemory
	var opt badger.Options
	if inMemory {
		opt = badger.DefaultOptions("").WithInMemory(true)
	} else {
		opt = badger.DefaultOptions(filepath.Join(os.TempDir(), fmt.Sprintf(".genji-transient-%d", time.Now().Unix()+rand.Int63())))
	}
	opt.Compression = options.None
	opt.MetricsEnabled = false
	opt.Logger = nil
	opt.DetectConflicts = false

	db, err := badger.OpenManaged(opt)
	if err != nil {
		return nil, err
	}

	s := TransientStore{
		DB: db,
	}

	err = s.Reset()
	if err != nil {
		return nil, err
	}

	return &s, nil
}

// Close the engine and underlying Badger database.
func (e *Engine) Close() error {
	return e.DB.Close()
}

// A Transaction uses Badger's transactions.
type Transaction struct {
	ctx       context.Context
	ng        *Engine
	tx        *badger.Txn
	writable  bool
	discarded bool
}

// Rollback the transaction. Can be used safely after commit.
func (t *Transaction) Rollback() error {
	t.tx.Discard()

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

	return t.tx.Commit()
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

	_, err := t.tx.Get(key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, errors.WithStack(ErrStoreNotFound)
		}

		return nil, err
	}

	pkey := buildStorePrefixKey(name)

	return &Store{
		ctx:      t.ctx,
		ng:       t.ng,
		tx:       t.tx,
		prefix:   pkey,
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
	_, err := t.tx.Get(key)
	if err == nil {
		return errors.WithStack(ErrStoreAlreadyExists)
	}
	if !errors.Is(err, badger.ErrKeyNotFound) {
		return err
	}

	return t.tx.Set(key, nil)
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

	err = t.tx.Delete(buildStoreKey([]byte(name)))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return errors.WithStack(ErrStoreNotFound)
	}

	return err
}
