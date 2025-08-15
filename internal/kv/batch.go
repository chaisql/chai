package kv

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

var _ Session = (*BatchSession)(nil)

var (
	tombStone = []byte{0}
)

type BatchSession struct {
	Store           *Store
	DB              *pebble.DB
	Batch           *pebble.Batch
	closed          bool
	rollbackSegment *RollbackSegment
	maxBatchSize    int
	keys            map[string]struct{}
}

func (s *Store) NewBatchSession() *BatchSession {
	// before creating a batch session, create a shared snapshot
	// at this point-in-time.
	s.LockSharedSnapshot()

	b := s.db.NewBatch()

	return &BatchSession{
		Store:           s,
		DB:              s.db,
		Batch:           b,
		rollbackSegment: s.rollbackSegment,
		maxBatchSize:    s.opts.MaxBatchSize,
		keys:            make(map[string]struct{}),
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
	if _, ok := s.keys[string(k)]; ok {
		err := s.applyBatch()
		if err != nil {
			return nil, err
		}
	}

	return get(s.DB, k)
}

// Exists returns whether a key exists and is visible by the current session.
func (s *BatchSession) Exists(k []byte) (bool, error) {
	if _, ok := s.keys[string(k)]; ok {
		return true, nil
	}

	s.applyBatch()

	return exists(s.DB, k)
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
	clear(s.keys)

	return nil
}

func (s *BatchSession) ensureBatchSize() error {
	if s.Batch.Len() < s.maxBatchSize {
		return nil
	}

	// The batch is too large. Insert the rollback segments and commit the batch.
	s.applyBatch()

	return nil
}

// Insert inserts a key-value pair. If it already exists, it returns ErrKeyAlreadyExists.
func (s *BatchSession) Insert(k, v []byte) error {
	if len(k) == 0 {
		return errors.New("cannot store empty key")
	}

	if len(v) == 0 {
		return errors.New("cannot store empty value")
	}

	ok, err := s.Exists(k)
	if err != nil {
		return err
	}
	if ok {
		return ErrKeyAlreadyExists
	}

	s.keys[string(k)] = struct{}{}

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

	s.keys[string(k)] = struct{}{}

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

	delete(s.keys, string(k))

	return s.ensureBatchSize()
}

// DeleteRange deletes all keys in the given range.
// This implementation deletes all keys one by one to simplify the rollback.
func (s *BatchSession) DeleteRange(start []byte, end []byte) error {
	it, err := s.Iterator(&IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	if err != nil {
		return err
	}
	defer it.Close()

	for it.First(); it.Valid(); it.Next() {
		err := s.Delete(it.Key())
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *BatchSession) Iterator(opts *IterOptions) (Iterator, error) {
	err := s.applyBatch()
	if err != nil {
		return nil, err
	}

	var popts *pebble.IterOptions
	if opts != nil {
		popts = &pebble.IterOptions{
			LowerBound: opts.LowerBound,
			UpperBound: opts.UpperBound,
		}
	}

	it, err := s.DB.NewIter(popts)

	return &iterator{
		Iterator: it,
	}, err
}
