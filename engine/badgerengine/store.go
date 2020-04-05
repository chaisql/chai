package badgerengine

import (
	"bytes"
	"errors"

	"github.com/asdine/genji/engine"
	"github.com/dgraph-io/badger/v2"
)

// A Store is an implementation of the engine.Store interface.
type Store struct {
	tx       *badger.Txn
	prefix   []byte
	writable bool
	name     string
}

func buildKey(prefix, k []byte) []byte {
	key := make([]byte, 0, len(prefix)+1+len(k))
	key = append(key, prefix...)
	key = append(key, separator)
	key = append(key, k...)
	return key
}

// Put stores a key value pair. If it already exists, it overrides it.
func (s *Store) Put(k, v []byte) error {
	if !s.writable {
		return engine.ErrTransactionReadOnly
	}

	if len(k) == 0 {
		return errors.New("cannot store empty key")
	}

	return s.tx.Set(buildKey(s.prefix, k), v)
}

// Get returns a value associated with the given key. If not found, returns engine.ErrKeyNotFound.
func (s *Store) Get(k []byte) ([]byte, error) {
	it, err := s.tx.Get(buildKey(s.prefix, k))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, engine.ErrKeyNotFound
		}

		return nil, err
	}

	return it.ValueCopy(nil)
}

// Delete a record by key. If not found, returns engine.ErrKeyNotFound.
func (s *Store) Delete(k []byte) error {
	if !s.writable {
		return engine.ErrTransactionReadOnly
	}

	key := buildKey(s.prefix, k)
	_, err := s.tx.Get(key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return engine.ErrKeyNotFound
		}

		return err
	}

	return s.tx.Delete(key)
}

// Truncate deletes all the records of the store.
func (s *Store) Truncate() error {
	if !s.writable {
		return engine.ErrTransactionReadOnly
	}

	_, err := s.tx.Get(buildStoreKey(s.name))
	if err == badger.ErrKeyNotFound {
		return engine.ErrStoreNotFound
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

// NewIterator uses a Badger iterator with default options.
// Only one iterator is allowed per read-write transaction.
func (s *Store) NewIterator(cfg engine.IteratorConfig) engine.Iterator {
	prefix := buildKey(s.prefix, nil)

	opt := badger.DefaultIteratorOptions
	opt.Prefix = prefix
	opt.Reverse = cfg.Reverse
	it := s.tx.NewIterator(opt)

	return &iterator{
		storePrefix: s.prefix,
		prefix:      prefix,
		it:          it,
		reverse:     cfg.Reverse,
		item:        badgerItem{prefix: prefix},
	}
}

type iterator struct {
	prefix      []byte
	storePrefix []byte
	it          *badger.Iterator
	reverse     bool
	item        badgerItem
}

func (it *iterator) Seek(pivot []byte) {
	var seek []byte

	if !it.reverse {
		seek = buildKey(it.storePrefix, pivot)
	} else {
		seek = buildKey(it.storePrefix, append(pivot, 0xFF))
	}

	it.it.Seek(seek)
}

func (it *iterator) Valid() bool {
	return it.it.ValidForPrefix(it.prefix)
}

func (it *iterator) Next() {
	it.it.Next()
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
