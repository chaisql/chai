package memoryengine

import (
	"errors"
	"sort"
	"strings"
	"sync"

	"github.com/asdine/genji/engine"
	"github.com/google/btree"
)

type Engine struct {
	closed bool
	stores map[string]*btree.BTree

	mu sync.RWMutex
}

func NewEngine() *Engine {
	return &Engine{
		stores: make(map[string]*btree.BTree),
	}
}

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

func (ng *Engine) Close() error {
	ng.mu.Lock()
	defer ng.mu.Unlock()
	if ng.closed {
		return errors.New("engine already closed")
	}

	ng.closed = true
	return nil
}

type transaction struct {
	ng         *Engine
	writable   bool
	onRollback []func() // called during a rollback
	onCommit   []func() // called during a commit
	terminated bool
}

func (tx *transaction) Rollback() error {
	if tx.terminated {
		return nil
	}

	for _, undo := range tx.onRollback {
		undo()
	}

	tx.terminated = true

	if tx.writable {
		tx.ng.mu.Unlock()
	} else {
		tx.ng.mu.RUnlock()
	}

	return nil
}

func (tx *transaction) Commit() error {
	if tx.terminated {
		return errors.New("transaction already terminated")
	}

	if !tx.writable {
		return engine.ErrTransactionReadOnly
	}

	tx.terminated = true

	if tx.writable {
		for _, fn := range tx.onCommit {
			fn()
		}

		tx.ng.mu.Unlock()
	} else {
		tx.ng.mu.RUnlock()
	}

	return nil
}

func (tx *transaction) Store(name string) (engine.Store, error) {
	tr, ok := tx.ng.stores[name]
	if !ok {
		return nil, engine.ErrStoreNotFound
	}

	return &storeTx{tx: tx, tr: tr}, nil
}

func (tx *transaction) ListStores(prefix string) ([]string, error) {
	list := make([]string, 0, len(tx.ng.stores))
	for name := range tx.ng.stores {
		if strings.HasPrefix(name, prefix) {
			list = append(list, name)
		}
	}

	sort.Strings(list)

	return list, nil
}

func (tx *transaction) CreateStore(name string) error {
	if !tx.writable {
		return engine.ErrTransactionReadOnly
	}

	_, err := tx.Store(name)
	if err == nil {
		return engine.ErrStoreAlreadyExists
	}

	tr := btree.New(3)

	tx.ng.stores[name] = tr

	tx.onRollback = append(tx.onRollback, func() {
		delete(tx.ng.stores, name)
	})

	return nil
}

func (tx *transaction) DropStore(name string) error {
	if !tx.writable {
		return engine.ErrTransactionReadOnly
	}

	rb, ok := tx.ng.stores[name]
	if !ok {
		return engine.ErrStoreNotFound
	}

	delete(tx.ng.stores, name)

	tx.onRollback = append(tx.onRollback, func() {
		tx.ng.stores[name] = rb
	})

	return nil
}
