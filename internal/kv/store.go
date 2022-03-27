package kv

import (
	"io"
	"os"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return &[]byte{}
	},
}

type Store struct {
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
	buf := bufferPool.Get().(*[]byte)
	if cap(*buf) < len(prefix)+len(k)+2 {
		*buf = make([]byte, 0, len(prefix)+len(k)+2)
	}
	key := (*buf)[:0]
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
	if !s.writable {
		return ErrTransactionReadOnly
	}

	if len(k) == 0 {
		return errors.New("cannot store empty key")
	}

	if len(v) == 0 {
		return errors.New("cannot store empty value")
	}

	key := BuildKey(s.Prefix, k)
	err := s.tx.batch.Set(key, v, nil)
	bufferPool.Put(&key)
	return err
}

// Get returns a value associated with the given key. If not found, returns ErrKeyNotFound.
func (s *Store) Get(k []byte) ([]byte, error) {
	var closer io.Closer
	var err error
	var value []byte
	key := BuildKey(s.Prefix, k)
	if s.tx.writable {
		value, closer, err = s.tx.batch.Get(key)
	} else {
		value, closer, err = s.ng.DB.Get(key)
	}
	bufferPool.Put(&key)
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

// Get returns a value associated with the given key. If not found, returns ErrKeyNotFound.
func (s *Store) Exists(k []byte) (bool, error) {
	var closer io.Closer
	var err error
	key := BuildKey(s.Prefix, k)
	if s.tx.writable {
		_, closer, err = s.tx.batch.Get(key)
	} else {
		_, closer, err = s.ng.DB.Get(key)
	}
	bufferPool.Put(&key)
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

// Delete a record by key. If not found, returns ErrKeyNotFound.
func (s *Store) Delete(k []byte) error {
	if !s.writable {
		return ErrTransactionReadOnly
	}

	key := BuildKey(s.Prefix, k)
	_, closer, err := s.tx.batch.Get(key)
	bufferPool.Put(&key)
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

func (s *Store) Iterator(opts *pebble.IterOptions) *Iterator {
	if opts == nil {
		lowerBound := BuildKey(s.Prefix, nil)
		upperBound := BuildKey(s.Prefix, nil)
		upperBound[len(s.Prefix)] = 0xff
		opts = &pebble.IterOptions{
			LowerBound: lowerBound,
			UpperBound: upperBound,
		}
	}
	var it *pebble.Iterator
	if s.tx.writable {
		it = s.tx.batch.NewIter(opts)
	} else {
		it = s.ng.DB.NewIter(opts)
	}

	return &Iterator{
		Iterator:   it,
		upperBound: opts.UpperBound,
		lowerBound: opts.LowerBound,
	}
}

type Iterator struct {
	*pebble.Iterator

	lowerBound, upperBound []byte
}

func (it *Iterator) Close() error {
	err := it.Iterator.Close()
	if it.lowerBound != nil {
		bufferPool.Put(&it.lowerBound)
	}
	if it.upperBound != nil {
		bufferPool.Put(&it.upperBound)
	}
	return err
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

func (s *TransientStore) Iterator(opts *pebble.IterOptions) *Iterator {
	it := s.batch.NewIter(opts)

	return &Iterator{
		Iterator: it,
	}
}

// Drop releases any resource (files, memory, etc.) used by a transient store.
func (s *TransientStore) Drop() error {
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
