package kv

import "github.com/cockroachdb/errors"

type Options struct {
	RollbackSegmentNamespace int64
	MaxBatchSize             int
	MaxTransientBatchSize    int
	Extra                    map[string]string
}

type Store interface {
	NewSnapshotSession() Session
	NewBatchSession() Session
	NewTransientSession() Session

	LockSharedSnapshot()
	UnlockSharedSnapshot()
	Rollback() error

	Close() error
}

type CommitOptionFunc = func(opt *CommitOption)

type CommitOption struct {
	NoSync bool
}

var NoSync = func(opt *CommitOption) {
	opt.NoSync = true
}

type Session interface {
	Commit(opts ...CommitOptionFunc) error

	Close() error
	// Insert inserts a key-value pair. If it already exists, it returns ErrKeyAlreadyExists.
	Insert(k, v []byte) error
	// Put stores a key-value pair. If it already exists, it overrides it.
	Put(k, v []byte) error
	// Get returns a value associated with the given key. If not found, returns ErrKeyNotFound.
	Get(k []byte) ([]byte, error)
	// Exists returns whether a key exists and is visible by the current session.
	Exists(k []byte) (bool, error)
	// Delete a record by key. If not found, returns ErrKeyNotFound.
	Delete(k []byte) error
	DeleteRange(start []byte, end []byte) error

	Iterator(start []byte, end []byte) Iterator
}

type Iterator interface {
	First() bool
	Next() bool

	Last() bool // reverse
	Prev() bool

	Valid() bool
	Error() error

	Key() []byte
	Value() []byte
	Close() error
}

type StoreEngine interface {
	New(opt Options) (Store, error)
}

var engines = map[string]StoreEngine{}

func RegisterEngine(engine string, store StoreEngine) {
	engines[engine] = store
}

func NewStore(engine string, opt Options) (Store, error) {
	if e, ok := engines[engine]; ok {
		return e.New(opt)
	}
	return nil, errors.Errorf("unknown engine %s", engine)
}
