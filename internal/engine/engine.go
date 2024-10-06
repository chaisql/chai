package engine

import "github.com/cockroachdb/errors"

// Common errors returned by the engine.
var (
	// ErrKeyNotFound is returned when the targeted key doesn't exist.
	ErrKeyNotFound = errors.New("key not found")

	// ErrKeyAlreadyExists is returned when the targeted key already exists.
	ErrKeyAlreadyExists = errors.New("key already exists")
)

type Engine interface {
	Close() error
	Rollback() error
	Recover() error
	LockSharedSnapshot()
	UnlockSharedSnapshot()
	CleanupTransientNamespaces() error
	NewSnapshotSession() Session
	NewBatchSession() Session
	NewTransientSession() Session
}

type Session interface {
	Commit() error
	Close() error
	// Insert inserts a key-value pair. If it already exists, it returns ErrKeyAlreadyExists.
	Insert(k, v []byte) error
	// Put stores a key-value pair. If it already exists, it overrides it.
	Put(k, v []byte) error
	// Get returns a value associated with the given key. If not found, returns ErrKeyNotFound.
	Get(k []byte) ([]byte, error)
	// Exists returns whether a key exists and is visible by the current session.
	Exists(k []byte) (bool, error)
	// Delete a record by key. If not found, returns ErrKeyNotFound.
	Delete(k []byte) error
	DeleteRange(start []byte, end []byte) error
	Iterator(opts *IterOptions) (Iterator, error)
}

type Iterator interface {
	Close() error
	First() bool
	Start(reverse bool) bool
	Last() bool
	End(reverse bool) bool
	Valid() bool
	Next() bool
	Prev() bool
	Move(reverse bool) bool
	Error() error
	Key() []byte
	Value() ([]byte, error)
}

type IterOptions struct {
	// LowerBound specifies the smallest key (inclusive) that the iterator will
	// return during iteration. If the iterator is seeked or iterated past this
	// boundary the iterator will return Valid()==false. Setting LowerBound
	// effectively truncates the key space visible to the iterator.
	LowerBound []byte
	// UpperBound specifies the largest key (exclusive) that the iterator will
	// return during iteration. If the iterator is seeked or iterated past this
	// boundary the iterator will return Valid()==false. Setting UpperBound
	// effectively truncates the key space visible to the iterator.
	UpperBound []byte
}
