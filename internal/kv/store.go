package kv

import (
	"math"
	"sync"

	"github.com/chaisql/chai/internal/pkg/atomic"
	"github.com/cockroachdb/pebble"
)

const (
	defaultMaxBatchSize              = 10 * 1024 * 1024 // 10MB
	defaultMaxTransientBatchSize int = 1 << 19          // 512KB
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

func (s *Store) Rollback() error {
	return s.rollbackSegment.Rollback()
}

func (s *Store) ResetRollbackSegment() error {
	return s.rollbackSegment.Reset()
}

func (s *Store) LockSharedSnapshot() {
	s.sharedSnapshot.Lock()
	s.sharedSnapshot.snapshot = &snapshot{
		snapshot: s.db.NewSnapshot(),
		refCount: atomic.NewCounter(0, math.MaxInt64, false),
	}
	s.sharedSnapshot.snapshot.Incr()
	s.sharedSnapshot.Unlock()
}

func (s *Store) UnlockSharedSnapshot() {
	s.sharedSnapshot.Lock()
	s.sharedSnapshot.snapshot.Done()
	s.sharedSnapshot.snapshot = nil
	s.sharedSnapshot.Unlock()
}
