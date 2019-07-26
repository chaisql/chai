package engine

import (
	"errors"

	"github.com/asdine/genji/index"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

// Common errors returned by the engine implementations.
var (
	// ErrTableNotFound must be returned when the targeted table doesn't exist.
	ErrTableNotFound = errors.New("table not found")

	// ErrTableAlreadyExists must be returned when attempting to create a table with the
	// same name as an existing one.
	ErrTableAlreadyExists = errors.New("table already exists")

	// ErrIndexNotFound must be returned when the targeted index doesn't exist.
	ErrIndexNotFound = errors.New("index not found")

	// ErrTableAlreadyExists must be returned when attempting to create an index with the
	// same name as an existing one.
	ErrIndexAlreadyExists = errors.New("index already exists")

	// ErrTransactionReadOnly must be returned when attempting to call write methods on a read-only transaction.
	ErrTransactionReadOnly = errors.New("transaction is read-only")
)

// An Engine is responsible for storing data.
// Implementations can choose to store data on disk, in memory, in the browser etc. using the algorithms
// and data structures of their choice.
// Engines must support read-only and read/write transactions.
type Engine interface {
	Begin(writable bool) (Transaction, error)
	Close() error
}

// A Transaction provides methods for managing the collection of tables and the transaction itself.
// Transaction is either read-only or read/write. Read-only transactions can be used to read tables
// and read/write ones can be used to read, create, delete and modify tables.
type Transaction interface {
	Rollback() error
	Commit() error
	Table(name string, codec record.Codec) (table.Table, error)
	CreateTable(name string) error
	DropTable(name string) error
	Index(table, name string) (index.Index, error)
	Indexes(table string) (map[string]index.Index, error)
	CreateIndex(table, field string) error
	DropIndex(table, field string) error
}
