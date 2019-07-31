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

// A Tx provides methods for managing the collection of stores and the transaction itself.
// Tx is either read-only or read/write. Read-only transactions can be used to read stores
// and read/write ones can be used to read, create, delete and modify stores.
type Tx interface {
	Rollback() error
	Commit() error
	Store(name string) (Store, error)
	CreateStore(name string) error
	DropStore(name string) error
}

// A Store manages key value pairs.
type Store interface {
	// Get returns a value associated with the given key. If no key is not found, it returns ErrKeyNotFound.
	Get(k []byte) ([]byte, error)
	// Put stores a key value pair. If it already exists, it overrides it.
	Put(k, v []byte) error
	// Delete a key value pair. If the key is not found, returns ErrKeyNotFound.
	Delete(k []byte) error
	// Truncate deletes all the key value pairs from the store.
	Truncate() error
	// AscendGreater seeks for the pivot and then goes through all the subsequent key value pairs in increasing order and calls the given function for each pair.
	// If the given function returns an error, the iteration stops and returns that error.
	// If the pivot is nil, starts from the beginning.
	AscendGreater(pivot []byte, fn func(k, v []byte) error) error
	// DescendGreater seeks for the pivot and then goes through all the subsequent key value pairs in descreasing order and calls the given function for each pair.
	// If the given function returns an error, the iteration stops and returns that error.
	// If the pivot is nil, starts from the end.
	DescendGreater(pivot []byte, fn func(k, v []byte) error) error
}
