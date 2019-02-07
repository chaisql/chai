package memory

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/table"
	"modernc.org/b"
)

type Engine struct {
	mu        sync.RWMutex
	closed    bool
	tables    map[string]*b.Tree
	txCounter uint64
}

func NewEngine() *Engine {
	return &Engine{
		tables: make(map[string]*b.Tree),
	}
}

func (ng *Engine) Begin(writable bool) (engine.Transaction, error) {
	if writable {
		ng.mu.Lock()
		defer ng.mu.Unlock()
	} else {
		ng.mu.RLock()
		defer ng.mu.RUnlock()
	}

	if ng.closed {
		return nil, errors.New("engine closed")
	}

	return &transaction{ng: ng, writable: writable, xid: atomic.AddUint64(&ng.txCounter, 1)}, nil
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
	ng        *Engine
	writable  bool
	xid       uint64
	mutations []*item
}

func (tx *transaction) Rollback() error {
	if !tx.writable {
		return nil
	}

	for _, it := range tx.mutations {
		it.rootTree.Delete(it.rowid)
	}

	return nil
}

func (tx *transaction) Commit() error {
	if !tx.writable {
		return nil
	}

	return nil
}

func (tx *transaction) Table(name string) (table.Table, error) {
	tr, ok := tx.ng.tables[name]
	if !ok {
		return nil, errors.New("table not found")
	}

	return &tableTx{writable: tx.writable, tree: tr}, nil
}
