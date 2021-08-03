package badgerengine

import (
	"bytes"
	"context"

	"github.com/genjidb/genji/internal/errors"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji/engine"
)

// A Store is an implementation of the engine.Store interface.
type Store struct {
	ctx      context.Context
	ng       *Engine
	tx       *badger.Txn
	prefix   []byte
	writable bool
	name     []byte
}

// build a long key for each key of a store
// in the form: storePrefix + <sep> + 0 + key.
// the 0 is used to separate the actual key
// from the rest of the prexix and to ensure
// we can quickly access the latest key of the store
// by replacing 0 by anything bigger.
func buildKey(prefix, k []byte) []byte {
	key := make([]byte, 0, len(prefix)+2+len(k))
	key = append(key, prefix...)
	key = append(key, separator)
	key = append(key, 0)
	key = append(key, k...)
	return key
}

// Put stores a key value pair. If it already exists, it overrides it.
func (s *Store) Put(k, v []byte) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}

	if !s.writable {
		return engine.ErrTransactionReadOnly
	}

	if len(k) == 0 {
		return errors.New("cannot store empty key")
	}

	if len(v) == 0 {
		return errors.New("cannot store empty value")
	}

	return s.tx.Set(buildKey(s.prefix, k), v)
}

// Get returns a value associated with the given key. If not found, returns engine.ErrKeyNotFound.
func (s *Store) Get(k []byte) ([]byte, error) {
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	default:
	}

	it, err := s.tx.Get(buildKey(s.prefix, k))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, errors.New(engine.ErrKeyNotFound)
		}

		return nil, err
	}

	return it.ValueCopy(nil)
}

// Delete a record by key. If not found, returns engine.ErrKeyNotFound.
func (s *Store) Delete(k []byte) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}

	if !s.writable {
		return engine.ErrTransactionReadOnly
	}

	key := buildKey(s.prefix, k)
	_, err := s.tx.Get(key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return errors.New(engine.ErrKeyNotFound)
		}

		return err
	}

	return s.tx.Delete(key)
}

// Truncate deletes all the records of the store.
func (s *Store) Truncate() error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}

	if !s.writable {
		return engine.ErrTransactionReadOnly
	}

	_, err := s.tx.Get(buildStoreKey(s.name))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return errors.New(engine.ErrStoreNotFound)
	}

	it := s.tx.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	prefix := buildStorePrefixKey(s.name)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		err = s.tx.Delete(it.Item().Key())
		if err != nil {
			return err
		}
	}

	return nil
}

// Iterator uses a Badger iterator with default options.
// Only one iterator is allowed per read-write transaction.
func (s *Store) Iterator(opts engine.IteratorOptions) engine.Iterator {
	prefix := buildKey(s.prefix, nil)

	opt := badger.DefaultIteratorOptions
	opt.Prefix = prefix
	opt.Reverse = opts.Reverse
	it := s.tx.NewIterator(opt)

	return &iterator{
		ctx:         s.ctx,
		storePrefix: s.prefix,
		prefix:      prefix,
		it:          it,
		reverse:     opts.Reverse,
		item:        badgerItem{prefix: prefix},
	}
}

type iterator struct {
	ctx         context.Context
	prefix      []byte
	storePrefix []byte
	it          *badger.Iterator
	reverse     bool
	item        badgerItem
	err         error
}

func (it *iterator) Seek(pivot []byte) {
	select {
	case <-it.ctx.Done():
		it.err = it.ctx.Err()
		return
	default:
	}

	var seek []byte

	if !it.reverse {
		seek = buildKey(it.storePrefix, pivot)
	} else {
		// if pivot is nil and reverse is true,
		// seek the largest key by replacing 0
		// by anything bigger, here 255
		if len(pivot) == 0 {
			seek = buildKey(it.storePrefix, pivot)
			seek[len(seek)-1] = 255
		} else {
			seek = buildKey(it.storePrefix, append(pivot, 0xFF))
		}
	}

	it.it.Seek(seek)
}

func (it *iterator) Valid() bool {
	return it.it.ValidForPrefix(it.prefix) && it.err == nil
}

func (it *iterator) Next() {
	it.it.Next()
}

func (it *iterator) Err() error {
	return it.err
}

func (it *iterator) Item() engine.Item {
	it.item.item = it.it.Item()

	return &it.item
}

func (it *iterator) Close() error {
	it.it.Close()
	return nil
}

type badgerItem struct {
	item   *badger.Item
	prefix []byte
}

func (i *badgerItem) Key() []byte {
	return bytes.TrimPrefix(i.item.Key(), i.prefix)
}

func (i *badgerItem) ValueCopy(buf []byte) ([]byte, error) {
	return i.item.ValueCopy(buf)
}
