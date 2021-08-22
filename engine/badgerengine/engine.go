// Package badgerengine implements a Badger engine.
package badgerengine

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/internal/errors"
)

const (
	separator   byte = 0x1F
	storeKey         = "__genji.store"
	storePrefix      = 's'
)

// Engine represents a Badger engine.
type Engine struct {
	DB *badger.DB

	transient bool
}

// NewEngine creates a Badger engine. It takes the same argument as Badger's Open function.
func NewEngine(opt badger.Options) (*Engine, error) {
	db, err := badger.Open(opt)
	if err != nil {
		return nil, err
	}

	return &Engine{
		DB: db,
	}, nil
}

// Begin creates a transaction using Badger's transaction API.
func (e *Engine) Begin(ctx context.Context, opts engine.TxOptions) (engine.Transaction, error) {
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

func (e *Engine) NewTransientEngine(ctx context.Context) (engine.Engine, error) {
	// build engine with fast options
	opt := badger.DefaultOptions(filepath.Join(os.TempDir(), fmt.Sprintf(".genji-transient-%d", time.Now().Unix()+rand.Int63())))
	opt.Compression = options.None

	ng, err := NewEngine(opt)
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

	_ = e.Close()

	return os.RemoveAll(e.DB.Opts().Dir)
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
		return engine.ErrTransactionDiscarded
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
		return engine.ErrTransactionDiscarded
	}

	if !t.writable {
		return engine.ErrTransactionReadOnly
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
func (t *Transaction) GetStore(name []byte) (engine.Store, error) {
	select {
	case <-t.ctx.Done():
		return nil, t.ctx.Err()
	default:
	}

	key := buildStoreKey(name)

	_, err := t.tx.Get(key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, errors.Wrap(engine.ErrStoreNotFound)
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
// If the store already exists, returns engine.ErrStoreAlreadyExists.
func (t *Transaction) CreateStore(name []byte) error {
	select {
	case <-t.ctx.Done():
		return t.ctx.Err()
	default:
	}

	if !t.writable {
		return errors.Wrap(engine.ErrTransactionReadOnly)
	}

	key := buildStoreKey(name)
	_, err := t.tx.Get(key)
	if err == nil {
		return errors.Wrap(engine.ErrStoreAlreadyExists)
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
		return errors.Wrap(engine.ErrTransactionReadOnly)
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
		return errors.Wrap(engine.ErrStoreNotFound)
	}

	return err
}
