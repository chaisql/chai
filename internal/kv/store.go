package kv

import (
	"context"
	"io"
	"os"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

type Store struct {
	ctx      context.Context
	ng       *Engine
	tx       *Transaction
	Prefix   []byte
	writable bool
	name     []byte
}

// build a long key for each key of a store
// in the form: storePrefix + <sep> + 0 + key.
// the 0 is used to separate the actual key
// from the rest of the prexix and to ensure
// we can quickly access the latest key of the store
// by replacing 0 by anything bigger.
func BuildKey(prefix, k []byte) []byte {
	key := make([]byte, 0, len(prefix)+2+len(k))
	key = append(key, prefix...)
	key = append(key, separator)
	key = append(key, 0)
	key = append(key, k...)
	return key
}

func TrimPrefix(k []byte, prefix []byte) []byte {
	return k[len(prefix)+2:]
}

// Put stores a key value pair. If it already exists, it overrides it.
func (s *Store) Put(k, v []byte) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}

	if !s.writable {
		return ErrTransactionReadOnly
	}

	if len(k) == 0 {
		return errors.New("cannot store empty key")
	}

	if len(v) == 0 {
		return errors.New("cannot store empty value")
	}

	return s.tx.batch.Set(BuildKey(s.Prefix, k), v, nil)
}

// Get returns a value associated with the given key. If not found, returns ErrKeyNotFound.
func (s *Store) Get(k []byte) ([]byte, error) {
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	default:
	}

	var closer io.Closer
	var err error
	var value []byte
	if s.tx.writable {
		value, closer, err = s.tx.batch.Get(BuildKey(s.Prefix, k))
	} else {
		value, closer, err = s.ng.DB.Get(BuildKey(s.Prefix, k))
	}
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

// Delete a record by key. If not found, returns ErrKeyNotFound.
func (s *Store) Delete(k []byte) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}

	if !s.writable {
		return ErrTransactionReadOnly
	}

	key := BuildKey(s.Prefix, k)
	_, closer, err := s.tx.batch.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return errors.WithStack(ErrKeyNotFound)
		}

		return err
	}
	err = closer.Close()
	if err != nil {
		return err
	}

	return s.tx.batch.Delete(key, nil)
}

// Truncate deletes all the records of the store.
func (s *Store) Truncate() error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}

	if !s.writable {
		return ErrTransactionReadOnly
	}

	_, closer, err := s.tx.batch.Get(buildStoreKey(s.name))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return errors.WithStack(ErrKeyNotFound)
		}

		return err
	}
	err = closer.Close()
	if err != nil {
		return err
	}

	prefix := buildStorePrefixKey(s.name)

	it := s.tx.batch.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	defer it.Close()

	for it.SeekGE(prefix); it.Valid(); it.Next() {
		err = s.tx.batch.Delete(it.Key(), nil)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) Iterator(opts *pebble.IterOptions) *pebble.Iterator {
	if opts == nil {
		lowerBound := BuildKey(s.Prefix, nil)
		upperBound := BuildKey(s.Prefix, nil)
		upperBound[len(s.Prefix)] = 0xff
		opts = &pebble.IterOptions{
			LowerBound: lowerBound,
			UpperBound: upperBound,
		}
	}
	if s.tx.writable {
		return s.tx.batch.NewIter(opts)
	} else {
		return s.ng.DB.NewIter(opts)
	}
}

// A TransientStore is an implementation of the *kv.Store interface.
type TransientStore struct {
	DB    *pebble.DB
	Path  string
	batch *pebble.Batch
}

// Put stores a key value pair. If it already exists, it overrides it.
func (s *TransientStore) Put(k, v []byte) error {
	if len(k) == 0 {
		return errors.New("cannot store empty key")
	}

	if len(v) == 0 {
		return errors.New("cannot store empty value")
	}

	if s.batch == nil {
		s.batch = s.DB.NewIndexedBatch()
	}

	return s.batch.Set(k, v, nil)
}

func (s *TransientStore) Iterator(opts *pebble.IterOptions) *pebble.Iterator {
	return s.batch.NewIter(opts)
}

// Drop releases any resource (files, memory, etc.) used by a transient store.
func (s *TransientStore) Drop(ctx context.Context) error {
	if s.batch != nil {
		_ = s.batch.Close()
	}

	_ = s.DB.Close()

	err := os.RemoveAll(s.Path)
	if err != nil {
		return err
	}

	s.batch = nil
	s.DB = nil
	return nil
}

// Reset resets the transient store to be reused.
func (s *TransientStore) Reset() error {
	if s.batch != nil {
		s.batch.Reset()
	}
	return nil
}
