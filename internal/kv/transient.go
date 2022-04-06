package kv

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// A TransientStore is an implementation of the *kv.Store interface.
type TransientStore struct {
	DB    *pebble.DB
	Path  string
	batch *pebble.Batch
}

// NewTransientStore creates a pebble db with fast options.
func NewTransientStore(opts *pebble.Options) (*TransientStore, error) {
	// build engine with fast options
	var inMemory bool
	if opts != nil {
		_, inMemory = opts.FS.(*vfs.MemFS)
	}

	opt := pebble.Options{
		DisableWAL: true,
	}

	var path string
	if inMemory {
		opt.FS = vfs.NewMem()
	} else {
		path = filepath.Join(os.TempDir(), fmt.Sprintf(".genji-transient-%d", time.Now().Unix()+rand.Int63()))
	}
	opt.Logger = nil

	db, err := pebble.Open(path, &opt)
	if err != nil {
		return nil, err
	}

	s := TransientStore{
		DB:    db,
		Path:  path,
		batch: db.NewIndexedBatch(),
	}

	err = s.Reset()
	if err != nil {
		return nil, err
	}

	return &s, nil
}

// Put stores a key value pair. If it already exists, it overrides it.
func (s *TransientStore) Put(k, v []byte) error {
	if len(k) == 0 {
		return errors.New("cannot store empty key")
	}

	if len(v) == 0 {
		return errors.New("cannot store empty value")
	}

	if s.batch == nil {
		s.batch = s.DB.NewIndexedBatch()
	}

	return s.batch.Set(k, v, nil)
}

func (s *TransientStore) Iterator(opts *pebble.IterOptions) *Iterator {
	it := s.batch.NewIter(opts)

	return &Iterator{
		Iterator: it,
	}
}

// Drop releases any resource (files, memory, etc.) used by a transient store.
func (s *TransientStore) Drop() error {
	if s.batch != nil {
		_ = s.batch.Close()
	}

	_ = s.DB.Close()

	err := os.RemoveAll(s.Path)
	if err != nil {
		return err
	}

	s.batch = nil
	s.DB = nil
	return nil
}

// Reset resets the transient store to be reused.
func (s *TransientStore) Reset() error {
	if s.batch != nil {
		s.batch.Reset()
	}
	return nil
}
