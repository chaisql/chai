package memoryengine

import (
	"context"

	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/internal/errors"
	"github.com/google/btree"
)

// The degree of btrees.
// This value is arbitrary and has been selected after
// a few benchmarks.
// It may be improved after thorough testing.
const btreeDegree = 12

// Engine is a simple memory engine implementation that stores data in
// an in-memory Btree. It is not thread safe.
type Engine struct {
	Closed    bool
	stores    map[string]*btree.BTree
	transient bool
}

// NewEngine creates an in-memory engine.
func NewEngine() *Engine {
	return &Engine{
		stores: make(map[string]*btree.BTree),
	}
}

// Begin creates a transaction.
func (ng *Engine) Begin(ctx context.Context, opts engine.TxOptions) (engine.Transaction, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if ng.Closed {
		return nil, errors.New("engine closed")
	}

	return &transaction{ctx: ctx, ng: ng, writable: opts.Writable}, nil
}

func (ng *Engine) NewTransientEngine(ctx context.Context) (engine.Engine, error) {
	e := NewEngine()
	e.transient = true
	return e, nil
}

func (ng *Engine) Drop(ctx context.Context) error {
	if !ng.transient {
		return errors.New("cannot drop persistent engine")
	}

	return nil
}

// Close the engine.
func (ng *Engine) Close() error {
	if ng.Closed {
		return errors.New("engine already closed")
	}

	ng.Closed = true
	return nil
}

// This implements the engine.Transaction type.
type transaction struct {
	ctx        context.Context
	ng         *Engine
	writable   bool
	onRollback []func() // called during a rollback
	onCommit   []func() // called during a commit
	terminated bool
}

// If the transaction is writable, rollback calls
// every function stored in the onRollback slice
// to undo every mutation done since the beginning
// of the transaction.
func (tx *transaction) Rollback() error {
	if tx.terminated {
		return errors.Wrap(engine.ErrTransactionDiscarded)
	}

	tx.terminated = true

	if tx.writable {
		for _, undo := range tx.onRollback {
			undo()
		}
	}

	select {
	case <-tx.ctx.Done():
		return tx.ctx.Err()
	default:
	}

	return nil
}

// If the transaction is writable, Commit calls
// every function stored in the onCommit slice
// to finalize every mutation done since the beginning
// of the transaction.
func (tx *transaction) Commit() error {
	if tx.terminated {
		return errors.Wrap(engine.ErrTransactionDiscarded)
	}

	if !tx.writable {
		return errors.Wrap(engine.ErrTransactionReadOnly)
	}

	select {
	case <-tx.ctx.Done():
		return tx.Rollback()
	default:
	}

	tx.terminated = true

	for _, fn := range tx.onCommit {
		fn()
	}

	return nil
}

func (tx *transaction) GetStore(name []byte) (engine.Store, error) {
	select {
	case <-tx.ctx.Done():
		return nil, tx.ctx.Err()
	default:
	}

	tr, ok := tx.ng.stores[string(name)]
	if !ok {
		return nil, errors.Wrap(engine.ErrStoreNotFound)
	}

	return &storeTx{tx: tx, tr: tr, name: string(name)}, nil
}

func (tx *transaction) CreateStore(name []byte) error {
	select {
	case <-tx.ctx.Done():
		return tx.ctx.Err()
	default:
	}

	if !tx.writable {
		return errors.Wrap(engine.ErrTransactionReadOnly)
	}

	_, err := tx.GetStore(name)
	if err == nil {
		return errors.Wrap(engine.ErrStoreAlreadyExists)
	}

	tx.ng.stores[string(name)] = btree.New(btreeDegree)

	// on rollback, remove the btree from the list of stores
	tx.onRollback = append(tx.onRollback, func() {
		delete(tx.ng.stores, string(name))
	})

	return nil
}

func (tx *transaction) DropStore(name []byte) error {
	select {
	case <-tx.ctx.Done():
		return tx.ctx.Err()
	default:
	}

	if !tx.writable {
		return errors.Wrap(engine.ErrTransactionReadOnly)
	}

	rb, ok := tx.ng.stores[string(name)]
	if !ok {
		return errors.Wrap(engine.ErrStoreNotFound)
	}

	delete(tx.ng.stores, string(name))

	// on rollback put back the btree to the list of stores
	tx.onRollback = append(tx.onRollback, func() {
		tx.ng.stores[string(name)] = rb
	})

	return nil
}
