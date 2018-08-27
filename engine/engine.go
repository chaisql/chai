package engine

import "errors"

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
	Table(name string) (Table, error)
}
