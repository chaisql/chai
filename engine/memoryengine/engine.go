package memoryengine

import (
	"errors"
	"sync"

	"github.com/genjidb/genji/engine"
	"github.com/google/btree"
)

// The degree of btrees.
// This value is arbitrary and has been selected after
// a few benchmarks.
// It may be improved after thorough testing.
const btreeDegree = 12

// Engine is a simple memory engine implementation that stores data in
// an in-memory Btree. It allows multiple readers and one single writer.
type Engine struct {
	closed bool
	stores map[string]*btree.BTree

	mu sync.RWMutex
}

// NewEngine creates an in-memory engine.
func NewEngine() *Engine {
	return &Engine{
		stores: make(map[string]*btree.BTree),
	}
}

// Begin creates a transaction.
func (ng *Engine) Begin(writable bool) (engine.Transaction, error) {
	if writable {
		ng.mu.Lock()
	} else {
		ng.mu.RLock()
	}

	if ng.closed {
		return nil, errors.New("engine closed")
	}

	return &transaction{ng: ng, writable: writable}, nil
}

// Close the engine.
func (ng *Engine) Close() error {
	ng.mu.Lock()
	defer ng.mu.Unlock()
	if ng.closed {
		return errors.New("engine already closed")
	}

	ng.closed = true
	return nil
}

// This implements the engine.Transaction type.
type transaction struct {
	ng         *Engine
	writable   bool
	onRollback []func() // called during a rollback
	onCommit   []func() // called during a commit
	terminated bool
	wg         sync.WaitGroup
}

// If the transaction is writable, rollback calls
// every function stored in the onRollback slice
// to undo every mutation done since the beginning
// of the transaction.
func (tx *transaction) Rollback() error {
	if tx.terminated {
		return nil
	}

	tx.terminated = true

	tx.wg.Wait()

	if tx.writable {
		for _, undo := range tx.onRollback {
			undo()
		}
		tx.ng.mu.Unlock()
	} else {
		tx.ng.mu.RUnlock()
	}

	return nil
}

// If the transaction is writable, Commit calls
// every function stored in the onCommit slice
// to finalize every mutation done since the beginning
// of the transaction.
func (tx *transaction) Commit() error {
	if tx.terminated {
		return errors.New("transaction already terminated")
	}

	if !tx.writable {
		return engine.ErrTransactionReadOnly
	}

	tx.wg.Wait()

	tx.terminated = true

	for _, fn := range tx.onCommit {
		fn()
	}

	tx.ng.mu.Unlock()

	return nil
}

func (tx *transaction) GetStore(name []byte) (engine.Store, error) {
	tr, ok := tx.ng.stores[string(name)]
	if !ok {
		return nil, engine.ErrStoreNotFound
	}

	return &storeTx{tx: tx, tr: tr}, nil
}

func (tx *transaction) CreateStore(name []byte) error {
	if !tx.writable {
		return engine.ErrTransactionReadOnly
	}

	_, err := tx.GetStore(name)
	if err == nil {
		return engine.ErrStoreAlreadyExists
	}

	tr := btree.New(btreeDegree)

	tx.ng.stores[string(name)] = tr

	// on rollback, remove the btree from the list of stores
	tx.onRollback = append(tx.onRollback, func() {
		delete(tx.ng.stores, string(name))
	})

	return nil
}

func (tx *transaction) DropStore(name []byte) error {
	if !tx.writable {
		return engine.ErrTransactionReadOnly
	}

	rb, ok := tx.ng.stores[string(name)]
	if !ok {
		return engine.ErrStoreNotFound
	}

	delete(tx.ng.stores, string(name))

	// on rollback put back the btree to the list of stores
	tx.onRollback = append(tx.onRollback, func() {
		tx.ng.stores[string(name)] = rb
	})

	return nil
}
