package badgerengine

import (
	"bytes"
	"errors"

	"github.com/dgraph-io/badger/v2"
	"github.com/genjidb/genji/engine"
)

// A Store is an implementation of the engine.Store interface.
type Store struct {
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

// NextSequence returns a monotonically increasing integer.
func (s *Store) NextSequence() (uint64, error) {
	if !s.writable {
		return 0, engine.ErrTransactionReadOnly
	}

	// TODO: this is an ineficient way of generating sequences.
	// use a bigger lease in the future.
	seq, err := s.ng.DB.GetSequence([]byte(s.name), 1)
	if err != nil {
		return 0, err
	}
	defer seq.Release()

	nb, err := seq.Next()
	if err != nil {
		return 0, err
	}

	// the first number in a Badger sequence is always zero
	// but Genji expects the first to be 1.
	return nb + 1, nil
}

// NewIterator uses a Badger iterator with default options.
// Only one iterator is allowed per read-write transaction.
func (s *Store) Iterator(opts engine.IteratorOptions) engine.Iterator {
	prefix := buildKey(s.prefix, nil)

	opt := badger.DefaultIteratorOptions
	opt.Prefix = prefix
	opt.Reverse = opts.Reverse
	it := s.tx.NewIterator(opt)

	return &iterator{
		storePrefix: s.prefix,
		prefix:      prefix,
		it:          it,
		reverse:     opts.Reverse,
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
	return it.it.ValidForPrefix(it.prefix)
}

func (it *iterator) Next() {
	it.it.Next()
}

func (it *iterator) Err() error {
	return nil
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
