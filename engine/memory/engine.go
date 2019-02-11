package memory

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/table"
	"modernc.org/b"
)

type Engine struct {
	closed  bool
	tables  map[string]*b.Tree
	indexes map[tableIndex]*Index

	mu sync.RWMutex
}

type tableIndex struct {
	table string
	index string
}

func NewEngine() *Engine {
	return &Engine{
		tables:  make(map[string]*b.Tree),
		indexes: make(map[tableIndex]*Index),
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

	tx.terminated = true

	if tx.writable {
		tx.ng.mu.Unlock()
	} else {
		tx.ng.mu.RUnlock()
	}

	return nil
}

func (tx *transaction) Table(name string) (table.Table, error) {
	tr, ok := tx.ng.tables[name]
	if !ok {
		return nil, errors.New("table not found")
	}

	return &tableTx{tx: tx, tree: tr}, nil
}

func (tx *transaction) CreateTable(name string) (table.Table, error) {
	if !tx.writable {
		return nil, errors.New("can't create table in read-only transaction")
	}

	_, err := tx.Table(name)
	if err == nil {
		return nil, fmt.Errorf("table '%s' already exists", name)
	}

	tr := b.TreeNew(func(a, b interface{}) int {
		return bytes.Compare(a.([]byte), b.([]byte))
	})

	tx.ng.tables[name] = tr

	tx.undos = append(tx.undos, func() {
		delete(tx.ng.tables, name)
	})

	return &tableTx{tx: tx, tree: tr}, nil
}

func (tx *transaction) Index(table, name string) (index.Index, error) {
	idx, ok := tx.ng.indexes[tableIndex{table, name}]
	if !ok {
		return nil, errors.New("index not found")
	}

	return idx, nil
}

func (tx *transaction) CreateIndex(table, name string) (index.Index, error) {
	if !tx.writable {
		return nil, errors.New("can't create index in read-only transaction")
	}

	_, err := tx.Index(table, name)
	if err == nil {
		return nil, fmt.Errorf("index '%s' already exists", name)
	}

	tx.ng.indexes[tableIndex{table, name}] = NewIndex()

	tx.undos = append(tx.undos, func() {
		delete(tx.ng.indexes, tableIndex{table, name})
	})

	return tx.ng.indexes[tableIndex{table, name}], nil
}
