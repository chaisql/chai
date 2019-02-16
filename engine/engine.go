package engine

import (
	"errors"

	"github.com/asdine/genji/index"
	"github.com/asdine/genji/table"
)

// Errors.
var (
	ErrTableNotFound       = errors.New("table not found")
	ErrIndexNotFound       = errors.New("index not found")
	ErrTransactionReadOnly = errors.New("transaction is read-only")
)

type Engine interface {
	Begin(writable bool) (Transaction, error)
	Close() error
}

type Transaction interface {
	Rollback() error
	Commit() error
	Table(name string) (table.Table, error)
	CreateTable(name string) (table.Table, error)
	Index(table, name string) (index.Index, error)
	Indexes(table string) (map[string]index.Index, error)
	CreateIndex(table, field string) (index.Index, error)
}
