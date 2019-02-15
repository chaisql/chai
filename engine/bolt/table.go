package bolt

import (
	"errors"

	"github.com/asdine/genji/engine"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	bolt "github.com/etcd-io/bbolt"
)

type Table struct {
	Bucket *bolt.Bucket
}

func (t *Table) Insert(r record.Record) ([]byte, error) {
	seq, err := t.Bucket.NextSequence()
	if err != nil {
		return nil, err
	}

	data, err := record.Encode(r)
	if err != nil {
		return nil, err
	}

	// TODO(asdine): encode in uint64 if that makes sense.
	rowid := field.EncodeInt64(int64(seq))

	err = t.Bucket.Put(rowid, data)
	if err != nil {
		return nil, err
	}

	return rowid, nil
}

func (t *Table) Record(rowid []byte) (record.Record, error) {
	v := t.Bucket.Get(rowid)
	if v == nil {
		return nil, engine.ErrNotFound
	}

	return record.EncodedRecord(v), nil
}

func (t *Table) Iterate(fn func(record.Record) bool) error {
	return t.Bucket.ForEach(func(_, v []byte) error {
		if v == nil {
			return nil
		}

		ok := fn(record.EncodedRecord(v))
		if !ok {
			return errors.New("iterate interrupted")
		}

		return nil
	})
}
