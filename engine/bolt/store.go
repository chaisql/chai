package bolt

import (
	"bytes"

	"github.com/asdine/genji/engine"
	bolt "github.com/etcd-io/bbolt"
)

// A Store is an implementation of the engine.Store interface using a bucket.
type Store struct {
	bucket *bolt.Bucket
	tx     *bolt.Tx
	name   []byte
}

// Put stores a key value pair. If it already exists, it overrides it.
func (s *Store) Put(k, v []byte) error {
	if !s.bucket.Writable() {
		return engine.ErrTransactionReadOnly
	}

	return s.bucket.Put(k, v)
}

// Get returns a value associated with the given key. If not found, returns engine.ErrKeyNotFound.
func (s *Store) Get(k []byte) ([]byte, error) {
	v := s.bucket.Get(k)
	if v == nil {
		return nil, engine.ErrKeyNotFound
	}

	return v, nil
}

// Delete a record by recordID. If not found, returns table.ErrRecordNotFound.
func (s *Store) Delete(k []byte) error {
	if !s.bucket.Writable() {
		return engine.ErrTransactionReadOnly
	}

	v := s.bucket.Get(k)
	if v == nil {
		return engine.ErrKeyNotFound
	}

	return s.bucket.Delete(k)
}

// AscendGreaterOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in increasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is nil, starts from the beginning.
func (s *Store) AscendGreaterOrEqual(pivot []byte, fn func(k, v []byte) error) error {
	c := s.bucket.Cursor()
	for k, v := c.Seek(pivot); k != nil; k, v = c.Next() {
		err := fn(k, v)
		if err != nil {
			return err
		}
	}

	return nil
}

// DescendLessOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in descreasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is nil, starts from the end.
func (s *Store) DescendLessOrEqual(pivot []byte, fn func(k, v []byte) error) error {
	var k, v []byte

	c := s.bucket.Cursor()
	if len(pivot) == 0 {
		k, v = c.Last()
	} else {
		k, v = c.Seek(pivot)
		if k == nil {
			k, v = c.Last()
		} else {
			for bytes.Compare(k, pivot) > 0 {
				k, v = c.Prev()
			}
		}
	}

	for k != nil {
		err := fn(k, v)
		if err != nil {
			return err
		}
		k, v = c.Prev()
	}

	return nil
}

// Truncate deletes all the records of the store.
func (s *Store) Truncate() error {
	if !s.bucket.Writable() {
		return engine.ErrTransactionReadOnly
	}

	err := s.tx.DeleteBucket(s.name)
	if err != nil {
		return err
	}

	_, err = s.tx.CreateBucket(s.name)
	return err
}
