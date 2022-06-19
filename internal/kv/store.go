package kv

import (
	"sync"

	"github.com/cockroachdb/pebble"
)

type Store struct {
	db              *pebble.DB
	opts            Options
	rollbackSegment *RollbackSegment

	// holds the shared snapshot read by all the read sessions
	// when a write session is open.
	// when no write session is open, the snapshot is nil
	// and every read session will use db.NewSnapshot()
	sharedSnapshot struct {
		sync.RWMutex

		snapshot *snapshot
	}
}

type Options struct {
	RollbackSegmentNamespace int64
	MaxBatchSize             int
	MaxTransientBatchSize    int
}

func NewStore(db *pebble.DB, opts Options) *Store {
	if opts.MaxBatchSize <= 0 {
		opts.MaxBatchSize = defaultMaxBatchSize
	}
	if opts.MaxTransientBatchSize <= 0 {
		opts.MaxTransientBatchSize = defaultMaxTransientBatchSize
	}

	return &Store{
		db:              db,
		opts:            opts,
		rollbackSegment: NewRollbackSegment(db, opts.RollbackSegmentNamespace),
	}
}

func (s *Store) NewSnapshotSession() *SnapshotSession {
	var sn *snapshot

	// if there is a shared snapshot, use it.
	s.sharedSnapshot.RLock()
	sn = s.sharedSnapshot.snapshot

	// if there is no shared snapshot, create one.
	if sn == nil {
		sn = &snapshot{
			snapshot: s.db.NewSnapshot(),
			refCount: 1,
		}
	} else {
		// if there is a shared snapshot, increment the ref count.
		sn.Incr()
	}

	s.sharedSnapshot.RUnlock()

	return &SnapshotSession{
		Store:    s,
		Snapshot: sn,
	}
}

func (s *Store) NewBatchSession() *BatchSession {
	// before creating a batch session, create a shared snapshot
	// at this point-in-time.
	s.LockSharedSnapshot()

	b := s.db.NewIndexedBatch()

	return &BatchSession{
		Store:           s,
		DB:              s.db,
		Batch:           b,
		rollbackSegment: s.rollbackSegment,
		maxBatchSize:    s.opts.MaxBatchSize,
	}
}

func (s *Store) NewTransientSession() *TransientSession {
	return &TransientSession{
		db:           s.db,
		maxBatchSize: s.opts.MaxTransientBatchSize,
	}
}

func (s *Store) Rollback() error {
	return s.rollbackSegment.Rollback()
}

func (s *Store) LockSharedSnapshot() {
	s.sharedSnapshot.Lock()
	s.sharedSnapshot.snapshot = &snapshot{
		snapshot: s.db.NewSnapshot(),
		refCount: 1,
	}
	s.sharedSnapshot.Unlock()
}

func (s *Store) UnlockSharedSnapshot() {
	s.sharedSnapshot.Lock()
	s.sharedSnapshot.snapshot.Done()
	s.sharedSnapshot.snapshot = nil
	s.sharedSnapshot.Unlock()
}
