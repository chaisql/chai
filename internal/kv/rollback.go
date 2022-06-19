package kv

import (
	"bytes"
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/genjidb/genji/internal/encoding"
)

const (
	kvOpSet = iota
	kvOpInsert
	kvOpDel
)

type RollbackSegment struct {
	db               *pebble.DB
	ops              []operation
	namespace        int64
	nsStart, nsEnd   []byte
	buf              []byte
	seen             map[string]struct{}
	segmentCommitted bool
}

type operation struct {
	key []byte
	op  byte
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

func (s *RollbackSegment) EnqueueOp(k []byte, kvOp uint8) {
	s.ops = append(s.ops, operation{
		key: k,
		op:  kvOp,
	})
}

func (s *RollbackSegment) Apply(b *pebble.Batch) error {
	if len(s.ops) == 0 {
		return nil
	}

	for _, op := range s.ops {
		s.buf = s.buf[:len(s.nsStart)]

		// seen keys are not added to the rollback segment
		if _, ok := s.seen[string(op.key)]; ok {
			continue
		}
		s.seen[string(op.key)] = struct{}{}

		var v []byte
		var closer io.Closer
		var err error
		if op.op != kvOpInsert {
			v, closer, err = s.db.Get(op.key)
			if err != nil {
				if err != pebble.ErrNotFound {
					return err
				}
			}
		}
		if v == nil {
			// key not found, add a tombstone to the rollback segment
			v = tombStone
		}

		// append the key to the buffer
		s.buf = encoding.EncodeBlob(s.buf, op.key)

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
	s.ops = s.ops[:0]
	s.buf = s.buf[:len(s.nsStart)]

	return nil
}

func (s *RollbackSegment) Rollback() error {
	if !s.segmentCommitted {
		return nil
	}

	// read the rollback segment and rollback the changes
	b := s.db.NewIndexedBatch()
	it := b.NewIter(&pebble.IterOptions{
		LowerBound: s.nsStart,
		UpperBound: s.nsEnd,
	})
	defer it.Close()

	for it.First(); it.Valid(); it.Next() {
		k := it.Key()

		// skip the namespace prefix
		n := encoding.Skip(k)
		k = k[n:]

		// get the key
		uk, _ := encoding.DecodeBlob(k)
		v := it.Value()

		var err error
		if bytes.Equal(v, tombStone) {
			err = b.Delete(uk, nil)
		} else {
			err = b.Set(uk, v, nil)
		}
		if err != nil {
			return err
		}
	}

	err := b.DeleteRange(s.nsStart, s.nsEnd, nil)
	if err != nil {
		return err
	}

	// we don't need to sync here.
	// in case of a crash, the rollback segment will be rolled back
	// during the next recovery.
	return b.Commit(pebble.NoSync)
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
	s.ops = s.ops[:0]
	s.buf = s.buf[:0]
	s.segmentCommitted = false
	for k := range s.seen {
		delete(s.seen, k)
	}
}
