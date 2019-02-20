package bolt

import (
	"errors"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	bolt "github.com/etcd-io/bbolt"
)

type Table struct {
	Bucket *bolt.Bucket
}

func (t *Table) Insert(r record.Record) (rowid []byte, err error) {
	if pker, ok := r.(table.Pker); ok {
		rowid, err = pker.Pk()
		if err != nil {
			return nil, err
		}
	} else {
		seq, err := t.Bucket.NextSequence()
		if err != nil {
			return nil, err
		}

		// TODO(asdine): encode in uint64 if that makes sense.
		rowid = field.EncodeInt64(int64(seq))
	}

	data, err := record.Encode(r)
	if err != nil {
		return nil, err
	}

	err = t.Bucket.Put(rowid, data)
	if err != nil {
		return nil, err
	}

	return rowid, nil
}

func (t *Table) Record(rowid []byte) (record.Record, error) {
	v := t.Bucket.Get(rowid)
	if v == nil {
		return nil, table.ErrRecordNotFound
	}

	return record.EncodedRecord(v), nil
}

func (t *Table) Delete(rowid []byte) error {
	v := t.Bucket.Get(rowid)
	if v == nil {
		return table.ErrRecordNotFound
	}

	return t.Bucket.Delete(rowid)
}

func (t *Table) Iterate(fn func([]byte, record.Record) bool) error {
	errInterrupt := errors.New("interrupted")

	err := t.Bucket.ForEach(func(k, v []byte) error {
		if v == nil {
			return nil
		}

		ok := fn(k, record.EncodedRecord(v))
		if !ok {
			return errInterrupt
		}

		return nil
	})

	if err != nil && err != errInterrupt {
		return err
	}

	return nil
}
