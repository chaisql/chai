package kv

import (
	"github.com/chaisql/chai/internal/engine"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

var _ engine.Session = (*BatchSession)(nil)

var (
	tombStone = []byte{0}
)

type BatchSession struct {
	Store           *PebbleEngine
	DB              *pebble.DB
	Batch           *pebble.Batch
	closed          bool
	rollbackSegment *RollbackSegment
	maxBatchSize    int
}

func (s *PebbleEngine) NewBatchSession() engine.Session {
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

func (s *BatchSession) Commit() error {
	if s.closed {
		return errors.New("already closed")
	}

	// We are about to commit the batch, we can empty
	// the rollback segment.
	err := s.rollbackSegment.Clear(s.Batch)
	if err != nil {
		return err
	}

	err = s.Batch.Commit(nil)
	if err != nil {
		return err
	}

	return s.Close()
}

func (s *BatchSession) Close() error {
	if s.closed {
		return errors.New("already closed")
	}
	s.closed = true

	s.Store.UnlockSharedSnapshot()

	return s.Batch.Close()
}

// Get returns a value associated with the given key. If not found, returns ErrKeyNotFound.
func (s *BatchSession) Get(k []byte) ([]byte, error) {
	return get(s.Batch, k)
}

// Exists returns whether a key exists and is visible by the current session.
func (s *BatchSession) Exists(k []byte) (bool, error) {
	return exists(s.Batch, k)
}

func (s *BatchSession) applyBatch() error {
	if s.Batch.Empty() {
		return nil
	}

	err := s.rollbackSegment.Apply(s.Batch)
	if err != nil {
		return err
	}

	// this is an intermediary commit that might be rolled back by the user
	// so we don't need durability here.
	err = s.Batch.Commit(pebble.NoSync)
	if err != nil {
		return err
	}

	// reset batch
	s.Batch.Reset()

	return nil
}

func (s *BatchSession) ensureBatchSize() error {
	if s.Batch.Len() < s.maxBatchSize {
		return nil
	}

	// The batch is too large. Insert the rollback segments and commit the batch.
	return s.applyBatch()
}

// Insert inserts a key-value pair. If it already exists, it returns ErrKeyAlreadyExists.
func (s *BatchSession) Insert(k, v []byte) error {
	if len(k) == 0 {
		return errors.New("cannot store empty key")
	}

	if len(v) == 0 {
		return errors.New("cannot store empty value")
	}

	ok, err := exists(s.Batch, k)
	if err != nil {
		return err
	}
	if ok {
		return engine.ErrKeyAlreadyExists
	}

	err = s.Batch.Set(k, v, nil)
	if err != nil {
		return err
	}

	return s.ensureBatchSize()
}

// Put stores a key value pair. If it already exists, it overrides it.
func (s *BatchSession) Put(k, v []byte) error {
	if len(k) == 0 {
		return errors.New("cannot store empty key")
	}

	if len(v) == 0 {
		return errors.New("cannot store empty value")
	}

	err := s.Batch.Set(k, v, nil)
	if err != nil {
		return err
	}

	return s.ensureBatchSize()
}

// Delete a record by key. If the key doesn't exist, it doesn't do anything.
func (s *BatchSession) Delete(k []byte) error {
	err := s.Batch.Delete(k, nil)
	if err != nil {
		return err
	}

	return s.ensureBatchSize()
}

// DeleteRange deletes all keys in the given range.
// This implementation deletes all keys one by one to simplify the rollback.
func (s *BatchSession) DeleteRange(start []byte, end []byte) error {
	err := s.Batch.DeleteRange(start, end, nil)
	if err != nil {
		return err
	}

	return s.ensureBatchSize()
}

func (s *BatchSession) Iterator(opts *engine.IterOptions) (engine.Iterator, error) {
	var popts *pebble.IterOptions
	if opts != nil {
		popts = &pebble.IterOptions{
			LowerBound: opts.LowerBound,
			UpperBound: opts.UpperBound,
		}
	}

	it, err := s.Batch.NewIter(popts)
	if err != nil {
		return nil, err
	}

	return &iterator{
		Iterator: it,
	}, err
}
