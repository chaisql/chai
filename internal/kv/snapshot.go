package kv

import (
	"math"

	"github.com/chaisql/chai/internal/pkg/atomic"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
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

func (s *Store) NewSnapshotSession() *SnapshotSession {
	var sn *snapshot

	// if there is a shared snapshot, use it.
	s.sharedSnapshot.RLock()
	sn = s.sharedSnapshot.snapshot

	// if there is no shared snapshot, create one.
	if sn == nil {
		sn = &snapshot{
			snapshot: s.db.NewSnapshot(),
			refCount: atomic.NewCounter(0, math.MaxInt64, false),
		}
	}
	sn.Incr()

	s.sharedSnapshot.RUnlock()

	return &SnapshotSession{
		Store:    s,
		Snapshot: sn,
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

func (s *SnapshotSession) Iterator(opts *IterOptions) (Iterator, error) {
	var popts *pebble.IterOptions
	if opts != nil {
		popts = &pebble.IterOptions{
			LowerBound: opts.LowerBound,
			UpperBound: opts.UpperBound,
		}
	}

	it, err := s.Snapshot.snapshot.NewIter(popts)

	return &iterator{
		Iterator: it,
	}, err
}
