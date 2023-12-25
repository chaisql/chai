package kv

import (
	"github.com/chaisql/chai/internal/engine"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

type iterator struct {
	*pebble.Iterator
}

func (i *iterator) Value() ([]byte, error) {
	return i.Iterator.ValueAndErr()
}

// Get returns a value associated with the given key. If not found, returns ErrKeyNotFound.
func get(r pebble.Reader, k []byte) ([]byte, error) {
	value, closer, err := r.Get(k)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, errors.WithStack(engine.ErrKeyNotFound)
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
	_, closer, err := r.Get(k)
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
