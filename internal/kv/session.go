package kv

import (
	"io"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

// Common errors returned by the engine implementations.
var (
	// ErrStoreNotFound is returned when the targeted store doesn't exist.
	ErrStoreNotFound = errors.New("store not found")

	// ErrStoreAlreadyExists must be returned when attempting to create a store with the
	// same name as an existing one.
	ErrStoreAlreadyExists = errors.New("store already exists")

	// ErrKeyNotFound is returned when the targeted key doesn't exist.
	ErrKeyNotFound = errors.New("key not found")
)

type Session interface {
	Commit() error
	Close() error
	// Put stores a key-value pair. If it already exists, it overrides it.
	Put(k, v []byte) error
	// Get returns a value associated with the given key. If not found, returns ErrKeyNotFound.
	Get(k []byte) ([]byte, error)
	// Exists returns whether a key exists and is visible by the current session.
	Exists(k []byte) (bool, error)
	// Delete a record by key. If not found, returns ErrKeyNotFound.
	Delete(k []byte) error
	DeleteRange(start []byte, end []byte) error
	Iterator(opts *pebble.IterOptions) *pebble.Iterator
}

// Get returns a value associated with the given key. If not found, returns ErrKeyNotFound.
func get(r pebble.Reader, k []byte) ([]byte, error) {
	var closer io.Closer
	var err error
	var value []byte
	value, closer, err = r.Get(k)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, errors.WithStack(ErrKeyNotFound)
		}

		return nil, err
	}

	cp := make([]byte, len(value))
	copy(cp, value)

	err = closer.Close()
	if err != nil {
		return nil, err
	}

	return cp, nil
}

// Exists returns whether a key exists and is visible by the current session.
func exists(r pebble.Reader, k []byte) (bool, error) {
	var closer io.Closer
	var err error
	_, closer, err = r.Get(k)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}

		return false, err
	}
	err = closer.Close()
	if err != nil {
		return false, err
	}
	return true, nil
}
