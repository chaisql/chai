package memory

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/table"
	"modernc.org/b"
)

type Engine struct {
	mu     sync.RWMutex
	closed bool
	tables map[string]*b.Tree
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
	ng       *Engine
	writable bool
	undos    []func()
}

func (tx *transaction) Rollback() error {
	if !tx.writable {
		return nil
	}

	for _, undo := range tx.undos {
		undo()
	}

	return nil
}

func (tx *transaction) Commit() error {
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
	_, err := tx.Table(name)
	if err == nil {
		return nil, fmt.Errorf("table '%s' already exists", name)
	}

	tr := b.TreeNew(func(a, b interface{}) int {
		return bytes.Compare(a.([]byte), b.([]byte))
	})

	tx.ng.tables[name] = tr
	return &tableTx{tx: tx, tree: tr}, nil
}
