package kv

import (
	"encoding/binary"
	"io"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return &[]byte{}
	},
}

type NamespaceID uint32

func (n NamespaceID) Bytes() []byte {
	buf := *(bufferPool.Get().(*[]byte))
	if len(buf) < 4 {
		// because this buffer is gonna get put back to the buffer pool
		// and potentially being used to construct other keys, we need to
		// make sure that the buffer is big enough to hold largest keys.
		buf = make([]byte, 4, 100)
	}

	binary.BigEndian.PutUint32(buf, uint32(n))
	return buf[:4]
}

func (n NamespaceID) UpperBound() []byte {
	n++
	return n.Bytes()
}

// PebbleReader is the interface that contains methods
// that are used by both pebble.DB and pebble.Batch.
type PebbleReader interface {
	Get(key []byte) (value []byte, closer io.Closer, err error)
	Set(key []byte, value []byte, opts *pebble.WriteOptions) error
	Delete(key []byte, opts *pebble.WriteOptions) error
	NewIter(o *pebble.IterOptions) *pebble.Iterator
}

type Session struct {
	Reader   pebble.Reader
	Writer   pebble.Writer
	readOnly bool
	closed   bool
}

func NewReadSession(db *pebble.DB) *Session {
	return &Session{
		Reader:   db.NewSnapshot(),
		readOnly: true,
	}
}

func NewSession(db *pebble.DB) *Session {
	b := db.NewIndexedBatch()

	return &Session{
		Reader: b,
		Writer: b,
	}
}

func (s *Session) Commit() error {
	if s.readOnly {
		return errors.New("cannot commit in read-only mode")
	}
	if s.closed {
		return errors.New("already closed")
	}

	s.closed = true
	return s.Reader.(*pebble.Batch).Commit(nil)
}

func (s *Session) Close() error {
	if s.readOnly {
		return errors.New("cannot close in read-only mode")
	}
	if s.closed {
		return errors.New("already closed")
	}
	s.closed = true

	return s.Reader.(*pebble.Batch).Close()
}

// GetNamespace returns a store by name.
func (s *Session) GetNamespace(key NamespaceID) *Namespace {
	return &Namespace{
		session:  s,
		ID:       key,
		readOnly: s.readOnly,
	}
}

type Namespace struct {
	ID       NamespaceID
	session  *Session
	readOnly bool
}

func BuildKey(nid NamespaceID, k []byte) []byte {
	buf := *(bufferPool.Get().(*[]byte))
	if len(buf) < len(k)+4 {
		if cap(buf) < len(k)+4 {
			buf = make([]byte, len(k)+4)
		} else {
			buf = buf[:len(k)+4]
		}
	}

	binary.BigEndian.PutUint32(buf, uint32(nid))
	copy(buf[4:], k)
	return buf[:len(k)+4]
}

// Put stores a key value pair. If it already exists, it overrides it.
func (s *Namespace) Put(k, v []byte) error {
	if s.readOnly {
		return errors.New("cannot put in read-only mode")
	}

	if len(k) == 0 {
		return errors.New("cannot store empty key")
	}

	if len(v) == 0 {
		return errors.New("cannot store empty value")
	}

	key := BuildKey(s.ID, k)
	err := s.session.Writer.Set(key, v, nil)
	bufferPool.Put(&key)
	return err
}

// Get returns a value associated with the given key. If not found, returns ErrKeyNotFound.
func (s *Namespace) Get(k []byte) ([]byte, error) {
	var closer io.Closer
	var err error
	var value []byte
	key := BuildKey(s.ID, k)
	value, closer, err = s.session.Reader.Get(key)
	bufferPool.Put(&key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, errors.WithStack(ErrKeyNotFound)
		}

		return nil, err
	}

	cp := make([]byte, len(value))
	copy(cp, value)

	err = closer.Close()
	if err != nil {
		return nil, err
	}

	return cp, nil
}

// Get returns a value associated with the given key. If not found, returns ErrKeyNotFound.
func (s *Namespace) Exists(k []byte) (bool, error) {
	var closer io.Closer
	var err error
	key := BuildKey(s.ID, k)
	_, closer, err = s.session.Reader.Get(key)
	bufferPool.Put(&key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}

		return false, err
	}
	err = closer.Close()
	if err != nil {
		return false, err
	}
	return true, nil
}

// Delete a record by key. If not found, returns ErrKeyNotFound.
func (s *Namespace) Delete(k []byte) error {
	if s.readOnly {
		return errors.New("cannot delete in read-only mode")
	}

	key := BuildKey(s.ID, k)
	_, closer, err := s.session.Reader.Get(key)
	bufferPool.Put(&key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return errors.WithStack(ErrKeyNotFound)
		}

		return err
	}
	err = closer.Close()
	if err != nil {
		return err
	}

	return s.session.Writer.Delete(key, nil)
}

// Truncate deletes all the records of the store.
func (s *Namespace) Truncate() error {
	if s.readOnly {
		return errors.New("cannot truncate in read-only mode")
	}

	it := s.Iterator(nil)
	defer it.Close()

	for it.SeekGE(s.ID.Bytes()); it.Valid(); it.Next() {
		err := s.session.Writer.Delete(it.Key(), nil)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Namespace) Iterator(opts *pebble.IterOptions) *Iterator {
	var iterator Iterator
	if opts == nil {
		opts = &pebble.IterOptions{
			LowerBound: s.ID.Bytes(),
			UpperBound: s.ID.UpperBound(),
		}
		iterator.lowerBound = opts.LowerBound
		iterator.upperBound = opts.UpperBound
	}

	iterator.Iterator = s.session.Reader.NewIter(opts)
	return &iterator
}

type Iterator struct {
	*pebble.Iterator

	lowerBound, upperBound []byte
}

func (it *Iterator) Close() error {
	err := it.Iterator.Close()
	if it.lowerBound != nil {
		bufferPool.Put(&it.lowerBound)
	}
	if it.upperBound != nil {
		bufferPool.Put(&it.upperBound)
	}
	return err
}
