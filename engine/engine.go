// Package engine defines interfaces to be implemented by engines in order to be compatible with Genji.
package engine

import (
	"errors"
)

// Common errors returned by the engine implementations.
var (
	// ErrTransactionReadOnly is returned when attempting to call write methods on a read-only transaction.
	ErrTransactionReadOnly = errors.New("transaction is read-only")

	// ErrStoreNotFound is returned when the targeted store doesn't exist.
	ErrStoreNotFound = errors.New("store not found")

	// ErrStoreAlreadyExists must be returned when attempting to create a store with the
	// same name as an existing one.
	ErrStoreAlreadyExists = errors.New("store already exists")

	// ErrKeyNotFound is returned when the targeted key doesn't exist.
	ErrKeyNotFound = errors.New("key not found")
)

// An Engine is responsible for storing data.
// Implementations can choose to store data on disk, in memory, in the browser etc. using the algorithms
// and data structures of their choice.
// Engines must support read-only and read/write transactions.
type Engine interface {
	// Begin returns a read-only or read/write transaction depending on whether writable is set to false
	// or true, respectively.
	// The behaviour of opening a transaction when another one is already opened depends on the implementation.
	Begin(writable bool) (Transaction, error)
	// Close the engine after ensuring all the transactions have completed.
	Close() error
}

// A Transaction provides methods for managing the collection of stores and the transaction itself.
// The transaction is either read-only or read/write. Read-only transactions can be used to read stores
// and read/write ones can be used to read, create, delete and modify stores.
// If the transaction is read-only, any call to a write method must return the ErrTransactionReadOnly error.
type Transaction interface {
	// Rollback the transaction and cancel any change made during its lifetime.
	// If the transaction was already commited or rolled back, no error is returned.
	Rollback() error
	// Commit the transaction and any change made during its lifetime.
	// If the transaction was already rolled back or commited, an error is returned.
	Commit() error
	// Fetch a store by name. If the store doesn't exist, it returns the ErrStoreNotFound error.
	GetStore(name string) (Store, error)
	// Create a store with the given name. If the store already exists, it returns ErrStoreAlreadyExists.
	CreateStore(name string) error
	// Drop a store by name. If the store doesn't exist, it returns ErrStoreNotFound.
	// It deletes all the values stored in it.
	DropStore(name string) error
	// Returns a list of store names lexicographically sorted.
	// If there are no stores, an empty slice is returned.
	ListStores(prefix string) ([]string, error)
}

// A Store manages key value pairs. It is an abstraction on top of any data structure that can provide
// random read, random write, and ordered sequential read.
type Store interface {
	// Get returns a value associated with the given key. If no key is not found, it returns ErrKeyNotFound.
	Get(k []byte) ([]byte, error)
	// Put stores a key value pair. If it already exists, it overrides it.
	Put(k, v []byte) error
	// Delete a key value pair. If the key is not found, returns ErrKeyNotFound.
	Delete(k []byte) error
	// Truncate deletes all the key value pairs from the store.
	Truncate() error
	// AscendGreaterOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in increasing order and calls the given function for each pair.
	// If the given function returns an error, the iteration stops and returns that error.
	// If the pivot is nil, starts from the beginning.
	AscendGreaterOrEqual(pivot []byte, fn func(k, v []byte) error) error
	// DescendLessOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in descreasing order and calls the given function for each pair.
	// If the given function returns an error, the iteration stops and returns that error.
	// If the pivot is nil, starts from the end.
	DescendLessOrEqual(pivot []byte, fn func(k, v []byte) error) error
	// NewIterator creates an iterator with the given config.
	NewIterator(IteratorConfig) Iterator
}

// IteratorConfig is used to configure an iterator upon creation.
type IteratorConfig struct {
	Reverse bool
}

// An Iterator iterates on keys of a store in lexicographic order.
type Iterator interface {
	// Seek moves the iterator to the selected key. If the key doesn't exist, it must move to the
	// next smallest key greater than k.
	Seek(k []byte)
	// Next moves the iterator to the next item. If there are no items left it must return false.
	Next() bool
	// Item returns the current item.
	Item() Item
	// Close releases the resources associated with the iterator.
	Close()
}

// An Item represents a key-value pair.
type Item interface {
	// Key returns the key of the item.
	// The key is only guaranteed to be valid until the next call to the Next method of
	// the iterator.
	Key() []byte
	// ValueCopy copies the key to the given byte slice returns it.
	// If the slice is not big enough, it must create a new one and return it.
	ValueCopy([]byte) ([]byte, error)
}
