package memory

import (
	"errors"
	"sync"

	"github.com/asdine/genji/engine"
	idx "github.com/asdine/genji/index"
	"github.com/asdine/genji/table"
	"github.com/google/btree"
)

type Engine struct {
	closed  bool
	tables  map[string]*table.RecordBuffer
	indexes map[tableIndex]*btree.BTree

	mu sync.RWMutex
}

type tableIndex struct {
	table string
	index string
}

func NewEngine() *Engine {
	return &Engine{
		tables:  make(map[string]*table.RecordBuffer),
		indexes: make(map[tableIndex]*btree.BTree),
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
	undos      []func()
	terminated bool
}

func (tx *transaction) Rollback() error {
	if tx.terminated {
		return nil
	}

	for _, undo := range tx.undos {
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
		tx.ng.mu.Unlock()
	} else {
		tx.ng.mu.RUnlock()
	}

	return nil
}

func (tx *transaction) Table(name string) (table.Table, error) {
	rb, ok := tx.ng.tables[name]
	if !ok {
		return nil, engine.ErrTableNotFound
	}

	return &tableTx{tx: tx, RecordBuffer: rb}, nil
}

func (tx *transaction) CreateTable(name string) (table.Table, error) {
	if !tx.writable {
		return nil, engine.ErrTransactionReadOnly
	}

	_, err := tx.Table(name)
	if err == nil {
		return nil, engine.ErrTableAlreadyExists
	}

	var rb table.RecordBuffer

	tx.ng.tables[name] = &rb

	tx.undos = append(tx.undos, func() {
		delete(tx.ng.tables, name)
	})

	return &tableTx{tx: tx, RecordBuffer: &rb}, nil
}

func (tx *transaction) Index(table, name string) (idx.Index, error) {
	_, err := tx.Table(table)
	if err != nil {
		return nil, engine.ErrTableNotFound
	}

	tree, ok := tx.ng.indexes[tableIndex{table, name}]
	if !ok {
		return nil, engine.ErrIndexNotFound
	}

	return &index{tree: tree, tx: tx}, nil
}

func (tx *transaction) Indexes(table string) (map[string]idx.Index, error) {
	if _, err := tx.Table(table); err != nil {
		return nil, err
	}

	m := make(map[string]idx.Index)

	for ti, tree := range tx.ng.indexes {
		if ti.table != table {
			continue
		}

		m[ti.index] = &index{tree: tree, tx: tx}
	}

	return m, nil
}

func (tx *transaction) CreateIndex(table, name string) (idx.Index, error) {
	if !tx.writable {
		return nil, engine.ErrTransactionReadOnly
	}

	_, err := tx.Table(table)
	if err != nil {
		return nil, err
	}

	_, err = tx.Index(table, name)
	if err == nil {
		return nil, engine.ErrIndexAlreadyExists
	}

	tree := btree.New(3)
	tx.ng.indexes[tableIndex{table, name}] = tree

	tx.undos = append(tx.undos, func() {
		delete(tx.ng.indexes, tableIndex{table, name})
	})

	return &index{tree: tree, tx: tx}, nil
}
