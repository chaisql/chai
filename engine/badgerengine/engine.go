// Package badgerengine implements a Badger engine.
package badgerengine

import (
	"bytes"
	"context"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji/engine"
)

const (
	separator   byte = 0x1F
	storeKey         = "__genji.store"
	storePrefix      = 's'
)

// Engine represents a Badger engine.
type Engine struct {
	DB *badger.DB
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
		if err == badger.ErrKeyNotFound {
			return nil, engine.ErrStoreNotFound
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
		return engine.ErrTransactionReadOnly
	}

	key := buildStoreKey(name)
	_, err := t.tx.Get(key)
	if err == nil {
		return engine.ErrStoreAlreadyExists
	}
	if err != badger.ErrKeyNotFound {
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
		return engine.ErrTransactionReadOnly
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
	if err == badger.ErrKeyNotFound {
		return engine.ErrStoreNotFound
	}

	return err
}
