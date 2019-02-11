package engine

import (
	"errors"

	"github.com/asdine/genji/index"

	"github.com/asdine/genji/table"
)

// Errors.
var (
	ErrNotFound = errors.New("not found")
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
	Index(name string) (index.Index, error)
	CreateIndex(name string) (index.Index, error)
}
