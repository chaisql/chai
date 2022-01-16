package kv

import (
	"bytes"
	"context"
	"os"

	"github.com/genjidb/genji/internal/errors"

	"github.com/dgraph-io/badger/v3"
)

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
		return ErrTransactionReadOnly
	}

	if len(k) == 0 {
		return errors.New("cannot store empty key")
	}

	if len(v) == 0 {
		return errors.New("cannot store empty value")
	}

	return s.tx.Set(buildKey(s.prefix, k), v)
}

// Get returns a value associated with the given key. If not found, returns ErrKeyNotFound.
func (s *Store) Get(k []byte) (*Item, error) {
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	default:
	}

	it, err := s.tx.Get(buildKey(s.prefix, k))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, errors.Wrap(ErrKeyNotFound)
		}

		return nil, err
	}

	return &Item{
		item: it,
	}, nil
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

	key := buildKey(s.prefix, k)
	_, err := s.tx.Get(key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return errors.Wrap(ErrKeyNotFound)
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
		return ErrTransactionReadOnly
	}

	_, err := s.tx.Get(buildStoreKey(s.name))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return errors.Wrap(ErrStoreNotFound)
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

// IteratorOptions is used to configure an iterator upon creation.
type IteratorOptions struct {
	Reverse bool
}

// Iterator uses a Badger iterator with default options.
// Only one iterator is allowed per read-write transaction.
func (s *Store) Iterator(opts IteratorOptions) *Iterator {
	prefix := buildKey(s.prefix, nil)

	opt := badger.DefaultIteratorOptions
	opt.Prefix = prefix
	opt.Reverse = opts.Reverse
	it := s.tx.NewIterator(opt)

	return &Iterator{
		ctx:         s.ctx,
		storePrefix: s.prefix,
		prefix:      prefix,
		it:          it,
		reverse:     opts.Reverse,
		item:        Item{prefix: prefix},
	}
}

type Iterator struct {
	ctx         context.Context
	transient   bool
	prefix      []byte
	storePrefix []byte
	it          *badger.Iterator
	reverse     bool
	item        Item
	err         error
}

func (it *Iterator) buildKey(pivot []byte) []byte {
	if it.transient {
		return pivot
	}

	return buildKey(it.storePrefix, pivot)
}

func (it *Iterator) Seek(pivot []byte) {
	select {
	case <-it.ctx.Done():
		it.err = it.ctx.Err()
		return
	default:
	}

	var seek []byte

	// if pivot is nil and reverse is true,
	// seek the largest key by replacing 0
	// by anything bigger, here 255
	if len(pivot) == 0 && it.reverse {
		seek = it.buildKey(pivot)
		if len(seek) == 0 {
			seek = []byte{255}
		} else {
			seek[len(seek)-1] = 255
		}
	} else {
		seek = it.buildKey(pivot)
	}

	it.it.Seek(seek)
}

func (it *Iterator) Valid() bool {
	return it.it.ValidForPrefix(it.prefix) && it.err == nil
}

func (it *Iterator) Next() {
	it.it.Next()
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Item() *Item {
	it.item.item = it.it.Item()

	return &it.item
}

func (it *Iterator) Close() error {
	it.it.Close()
	return nil
}

type Item struct {
	item   *badger.Item
	prefix []byte
}

func (i *Item) Key() []byte {
	return bytes.TrimPrefix(i.item.Key(), i.prefix)
}

func (i *Item) ValueCopy(buf []byte) ([]byte, error) {
	return i.item.ValueCopy(buf)
}

// A TransientStore is an implementation of the *kv.Store interface.
type TransientStore struct {
	DB *badger.DB
	tx *badger.Txn

	hasCommitted bool
}

// Put stores a key value pair. If it already exists, it overrides it.
func (s *TransientStore) Put(k, v []byte) error {
	if len(k) == 0 {
		return errors.New("cannot store empty key")
	}

	if len(v) == 0 {
		return errors.New("cannot store empty value")
	}

	err := s.tx.Set(k, v)
	if err != badger.ErrTxnTooBig {
		return err
	}

	// commit the transaction and start a new one
	err = s.tx.Commit()
	if err != nil {
		return err
	}
	s.hasCommitted = true

	s.tx = s.DB.NewTransaction(true)
	return s.tx.Set(k, v)
}

// Get returns a value associated with the given key. If not found, returns ErrKeyNotFound.
func (s *TransientStore) Get(k []byte) (*Item, error) {
	panic("not implemented")
}

// Delete a record by key. If not found, returns ErrKeyNotFound.
func (s *TransientStore) Delete(k []byte) error {
	panic("not implemented")
}

// Truncate deletes all the records of the store.
func (s *TransientStore) Truncate() error {
	panic("not implemented")
}

// Iterator uses a Badger iterator with default options.
// Only one iterator is allowed per read-write transaction.
func (s *TransientStore) Iterator(opts IteratorOptions) *Iterator {
	opt := badger.DefaultIteratorOptions
	opt.Reverse = opts.Reverse

	it := s.tx.NewIterator(opt)

	return &Iterator{
		transient: true,
		ctx:       context.TODO(),
		it:        it,
		reverse:   opts.Reverse,
		item:      Item{},
	}
}

// Drop releases any resource (files, memory, etc.) used by a transient store.
func (s *TransientStore) Drop(ctx context.Context) error {
	_ = s.DB.Close()

	err := os.RemoveAll(s.DB.Opts().Dir)
	if err != nil {
		return err
	}

	s.tx = nil
	s.DB = nil
	return nil
}

// Reset resets the transient store to be reused.
func (s *TransientStore) Reset() error {
	if s.tx != nil {
		s.tx.Discard()
		if s.hasCommitted {
			s.tx.Discard()
			err := s.DB.DropAll()
			if err != nil {
				return err
			}
		}
	}

	s.hasCommitted = false
	s.tx = s.DB.NewTransaction(true)

	return nil
}