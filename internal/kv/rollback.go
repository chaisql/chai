package kv

import (
	"bytes"
	"errors"
	"io"

	"github.com/chaisql/chai/internal/encoding"
	"github.com/cockroachdb/pebble"
)

type RollbackSegment struct {
	db               *pebble.DB
	namespace        int64
	nsStart, nsEnd   []byte
	buf              []byte
	seen             map[string]struct{}
	segmentCommitted bool
}

func NewRollbackSegment(db *pebble.DB, namespace int64) *RollbackSegment {
	return &RollbackSegment{
		db:        db,
		namespace: namespace,
		nsStart:   encoding.EncodeInt(nil, namespace),
		nsEnd:     encoding.EncodeInt(nil, namespace+1),
		buf:       encoding.EncodeInt(nil, namespace),
		seen:      make(map[string]struct{}),
	}
}

func (s *RollbackSegment) Apply(b *pebble.Batch) error {
	r, n := pebble.ReadBatch(b.Repr())

	for i := uint32(0); i < n; i++ {
		s.buf = s.buf[:len(s.nsStart)]

		kind, key, _, ok, err := r.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}

		if _, ok := s.seen[string(key)]; ok {
			continue
		}
		s.seen[string(key)] = struct{}{}

		var v []byte
		var closer io.Closer

		switch kind {
		case pebble.InternalKeyKindDelete, pebble.InternalKeyKindSet:
			v, closer, err = s.db.Get(key)
			if err != nil {
				if !errors.Is(err, pebble.ErrNotFound) {
					return err
				}
			}
		default:
			continue
		}

		if v == nil {
			// key not found, add a tombstone to the rollback segment
			v = tombStone
		}

		// append the key to the buffer
		s.buf = encoding.EncodeBlob(s.buf, key)

		err = b.Set(s.buf, v, nil)
		if err != nil {
			return err
		}

		if closer != nil {
			err = closer.Close()
			if err != nil {
				return err
			}
		}
	}

	s.segmentCommitted = true
	s.buf = s.buf[:len(s.nsStart)]

	return nil
}

func (s *RollbackSegment) Rollback() error {
	if !s.segmentCommitted {
		return nil
	}

	// read the rollback segment and rollback the changes
	b := s.db.NewBatch()
	it, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: s.nsStart,
		UpperBound: s.nsEnd,
	})
	if err != nil {
		return err
	}
	defer func(it *pebble.Iterator) {
		_ = it.Close()
	}(it)

	for it.First(); it.Valid(); it.Next() {
		k := it.Key()

		// skip the namespace prefix
		n := encoding.Skip(k)
		k = k[n:]

		// get the key
		uk, _ := encoding.DecodeBlob(k)
		v, err := it.ValueAndErr()

		if bytes.Equal(v, tombStone) {
			err = b.Delete(uk, nil)
		} else {
			err = b.Set(uk, v, nil)
		}
		if err != nil {
			return err
		}
	}

	err = b.DeleteRange(s.nsStart, s.nsEnd, nil)
	if err != nil {
		return err
	}

	// we don't need to sync here.
	// in case of a crash, the rollback segment will be rolled back
	// during the next recovery phase.
	return b.Commit(pebble.NoSync)
}

func (s *RollbackSegment) Reset() error {
	s.segmentCommitted = true
	return s.Rollback()
}

func (s *RollbackSegment) Clear(b *pebble.Batch) error {
	if s.segmentCommitted {
		err := b.DeleteRange(s.nsStart, s.nsEnd, nil)
		if err != nil {
			return err
		}
	}

	s.reset()

	return nil
}

func (s *RollbackSegment) reset() {
	s.buf = s.buf[:len(s.nsStart)]
	s.segmentCommitted = false
}
