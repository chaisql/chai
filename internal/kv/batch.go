package kv

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

var _ Session = (*BatchSession)(nil)

type BatchSession struct {
	Batch  *pebble.Batch
	closed bool
}

func NewBatchSession(db *pebble.DB) *BatchSession {
	b := db.NewIndexedBatch()

	return &BatchSession{
		Batch: b,
	}
}

func (s *BatchSession) Commit() error {
	if s.closed {
		return errors.New("already closed")
	}

	s.closed = true
	return s.Batch.Commit(nil)
}

func (s *BatchSession) Close() error {
	if s.closed {
		return errors.New("already closed")
	}
	s.closed = true

	return s.Batch.Close()
}

// Put stores a key value pair. If it already exists, it overrides it.
func (s *BatchSession) Put(k, v []byte) error {
	if len(k) == 0 {
		return errors.New("cannot store empty key")
	}

	if len(v) == 0 {
		return errors.New("cannot store empty value")
	}

	return s.Batch.Set(k, v, nil)
}

// Get returns a value associated with the given key. If not found, returns ErrKeyNotFound.
func (s *BatchSession) Get(k []byte) ([]byte, error) {
	return get(s.Batch, k)
}

// Exists returns whether a key exists and is visible by the current session.
func (s *BatchSession) Exists(k []byte) (bool, error) {
	return exists(s.Batch, k)
}

// Delete a record by key. If not found, returns ErrKeyNotFound.
func (s *BatchSession) Delete(k []byte) error {
	_, closer, err := s.Batch.Get(k)
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

	return s.Batch.Delete(k, nil)
}

func (s *BatchSession) DeleteRange(start []byte, end []byte) error {
	return s.Batch.DeleteRange(start, end, nil)
}

func (s *BatchSession) Iterator(opts *pebble.IterOptions) *pebble.Iterator {
	return s.Batch.NewIter(opts)
}
