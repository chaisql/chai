package kv

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

type SnapshotSession struct {
	Snapshot *pebble.Snapshot
	closed   bool
}

var _ Session = (*SnapshotSession)(nil)

func NewSnapshotSession(db *pebble.DB) *SnapshotSession {
	return &SnapshotSession{
		Snapshot: db.NewSnapshot(),
	}
}

func (s *SnapshotSession) Commit() error {
	return errors.New("cannot commit in read-only mode")
}

func (s *SnapshotSession) Close() error {
	if s.closed {
		return errors.New("already closed")
	}
	s.closed = true

	return s.Snapshot.Close()
}

func (s *SnapshotSession) Put(k, v []byte) error {
	return errors.New("cannot put in read-only mode")
}

// Get returns a value associated with the given key. If not found, returns ErrKeyNotFound.
func (s *SnapshotSession) Get(k []byte) ([]byte, error) {
	return get(s.Snapshot, k)
}

// Exists returns whether a key exists and is visible by the current session.
func (s *SnapshotSession) Exists(k []byte) (bool, error) {
	return exists(s.Snapshot, k)
}

// Delete a record by key. If not found, returns ErrKeyNotFound.
func (s *SnapshotSession) Delete(k []byte) error {
	return errors.New("cannot delete in read-only mode")
}

func (s *SnapshotSession) DeleteRange(start []byte, end []byte) error {
	return errors.New("cannot delete range in read-only mode")
}

func (s *SnapshotSession) Iterator(opts *pebble.IterOptions) *pebble.Iterator {
	return s.Snapshot.NewIter(opts)
}
