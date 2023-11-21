package kv

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/genjidb/genji/lib/atomic"
)

type snapshot struct {
	refCount *atomic.Counter
	snapshot *pebble.Snapshot
}

func (s *snapshot) Incr() {
	s.refCount.Incr()
}

func (s *snapshot) Done() error {
	if s.refCount.Decr() <= 0 {
		return s.snapshot.Close()
	}
	return nil
}

type SnapshotSession struct {
	Store    *Store
	Snapshot *snapshot
	closed   bool
}

var _ Session = (*SnapshotSession)(nil)

func (s *SnapshotSession) Commit() error {
	return errors.New("cannot commit in read-only mode")
}

func (s *SnapshotSession) Close() error {
	if s.closed {
		return errors.New("already closed")
	}
	s.closed = true

	return s.Snapshot.Done()
}

func (s *SnapshotSession) Insert(k, v []byte) error {
	return errors.New("cannot insert in read-only mode")
}

func (s *SnapshotSession) Put(k, v []byte) error {
	return errors.New("cannot put in read-only mode")
}

// Get returns a value associated with the given key. If not found, returns ErrKeyNotFound.
func (s *SnapshotSession) Get(k []byte) ([]byte, error) {
	return get(s.Snapshot.snapshot, k)
}

// Exists returns whether a key exists and is visible by the current session.
func (s *SnapshotSession) Exists(k []byte) (bool, error) {
	return exists(s.Snapshot.snapshot, k)
}

// Delete a record by key. If not found, returns ErrKeyNotFound.
func (s *SnapshotSession) Delete(k []byte) error {
	return errors.New("cannot delete in read-only mode")
}

func (s *SnapshotSession) DeleteRange(start []byte, end []byte) error {
	return errors.New("cannot delete range in read-only mode")
}

func (s *SnapshotSession) Iterator(opts *pebble.IterOptions) (*pebble.Iterator, error) {
	return s.Snapshot.snapshot.NewIter(opts)
}
