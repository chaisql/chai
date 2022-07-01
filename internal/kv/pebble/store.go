package pebble

import (
	"github.com/cockroachdb/pebble"
	"github.com/genjidb/genji/internal/kv"
	"sync"
)

type store struct {
	db              *pebble.DB
	opts            kv.Options
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

func (s *store) Close() error {
	return s.db.Close()
}

func NewStore(db *pebble.DB, opts kv.Options) kv.Store {
	if opts.MaxBatchSize <= 0 {
		opts.MaxBatchSize = defaultMaxBatchSize
	}
	if opts.MaxTransientBatchSize <= 0 {
		opts.MaxTransientBatchSize = defaultMaxTransientBatchSize
	}

	return &store{
		db:              db,
		opts:            opts,
		rollbackSegment: NewRollbackSegment(db, opts.RollbackSegmentNamespace),
	}
}

func (s *store) NewSnapshotSession() kv.Session {
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

func (s *store) NewBatchSession() kv.Session {
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

func (s *store) NewTransientSession() kv.Session {
	return &TransientSession{
		db:           s.db,
		maxBatchSize: s.opts.MaxTransientBatchSize,
	}
}

func (s *store) Rollback() error {
	return s.rollbackSegment.Rollback()
}

func (s *store) LockSharedSnapshot() {
	s.sharedSnapshot.Lock()
	s.sharedSnapshot.snapshot = &snapshot{
		snapshot: s.db.NewSnapshot(),
		refCount: 1,
	}
	s.sharedSnapshot.Unlock()
}

func (s *store) UnlockSharedSnapshot() {
	s.sharedSnapshot.Lock()
	s.sharedSnapshot.snapshot.Done()
	s.sharedSnapshot.snapshot = nil
	s.sharedSnapshot.Unlock()
}
