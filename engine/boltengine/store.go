package boltengine

import (
	"bytes"
	"context"

	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/internal/errors"
	bolt "go.etcd.io/bbolt"
)

// A Store is an implementation of the engine.Store interface using a bucket.
type Store struct {
	bucket *bolt.Bucket
	tx     *bolt.Tx
	ngTx   *Transaction
	name   []byte
	ctx    context.Context
}

// Put stores a key value pair. If it already exists, it overrides it.
func (s *Store) Put(k, v []byte) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}

	if !s.bucket.Writable() {
		return errors.Wrap(engine.ErrTransactionReadOnly)
	}

	if len(k) == 0 || len(v) == 0 {
		return errors.New("empty key or value")
	}

	return s.bucket.Put(k, v)
}

// Get returns a value associated with the given key. If not found, returns engine.ErrKeyNotFound.
func (s *Store) Get(k []byte) ([]byte, error) {
	select {
	case <-s.ctx.Done():
		return nil, errors.Wrap(s.ctx.Err())
	default:
	}

	v := s.bucket.Get(k)
	if v == nil {
		return nil, errors.Wrap(engine.ErrKeyNotFound)
	}

	return v, nil
}

// Delete a record by key. If not found, returns table.ErrDocumentNotFound.
// It hides the key without deleting the actual node from the tree, to tree rebalancing during iterations.
// It then adds it to a sub bucket containing the list of keys to delete when the transaction
// is committed.
func (s *Store) Delete(k []byte) error {
	select {
	case <-s.ctx.Done():
		return errors.Wrap(s.ctx.Err())
	default:
	}

	if !s.bucket.Writable() {
		return errors.Wrap(engine.ErrTransactionReadOnly)
	}

	v := s.bucket.Get(k)
	if v == nil {
		return errors.Wrap(engine.ErrKeyNotFound)
	}

	// setting the value to nil hides the key
	// without deleting the actual node from the tree.
	err := s.bucket.Put(k, nil)
	if err != nil {
		return errors.Wrap(err)
	}

	// mark the key for deletion on commit
	return errors.Wrap(s.ngTx.markForDeletion(s.name, k))
}

// Truncate deletes all the records of the store.
func (s *Store) Truncate() error {
	select {
	case <-s.ctx.Done():
		return errors.Wrap(s.ctx.Err())
	default:
	}

	if !s.bucket.Writable() {
		return errors.Wrap(engine.ErrTransactionReadOnly)
	}

	err := s.tx.DeleteBucket(s.name)
	if err != nil {
		return errors.Wrap(err)
	}

	_, err = s.tx.CreateBucket(s.name)
	return errors.Wrap(err)
}

// Iterator uses the Bolt bucket cursor.
func (s *Store) Iterator(opts engine.IteratorOptions) engine.Iterator {
	return &iterator{
		c:       s.bucket.Cursor(),
		reverse: opts.Reverse,
		ctx:     s.ctx,
	}
}

type iterator struct {
	c       *bolt.Cursor
	reverse bool
	item    boltItem
	err     error
	ctx     context.Context
}

func (it *iterator) Seek(pivot []byte) {
	select {
	case <-it.ctx.Done():
		it.err = errors.Wrap(it.ctx.Err())
		return
	default:
	}

	if !it.reverse {
		it.item.k, it.item.v = it.c.Seek(pivot)
		if it.item.v == nil {
			it.getKey(it.c.Next)
		}
		return
	}

	if len(pivot) == 0 {
		it.item.k, it.item.v = it.c.Last()
		if it.item.v == nil {
			it.getKey(it.c.Prev)
		}
		return
	}

	it.item.k, it.item.v = it.c.Seek(pivot)
	if it.item.k != nil {
		for bytes.Compare(it.item.k, pivot) > 0 || (len(it.item.k) > 0 && len(it.item.v) == 0) {
			it.item.k, it.item.v = it.c.Prev()
		}
	}
}

func (it *iterator) Valid() bool {
	return it.item.k != nil && it.err == nil
}

func (it *iterator) Next() {
	if it.reverse {
		it.getKey(it.c.Prev)
	} else {
		it.getKey(it.c.Next)
	}
}

func (it *iterator) getKey(fn func() (key []byte, value []byte)) {
	// skip items with nil value.
	// these items have been soft-deleted and will be removed on commit.
	for {
		it.item.k, it.item.v = fn()

		if it.item.k == nil || len(it.item.v) != 0 {
			break
		}
	}
}

func (it *iterator) Err() error {
	return errors.Wrap(it.err)
}

func (it *iterator) Item() engine.Item {
	if it.item.k == nil {
		return nil
	}

	return &it.item
}

func (it *iterator) Close() error { return nil }

type boltItem struct {
	k, v []byte
}

func (i *boltItem) Key() []byte {
	return i.k
}

func (i *boltItem) ValueCopy(buf []byte) ([]byte, error) {
	return append(buf[:0], i.v...), nil
}
