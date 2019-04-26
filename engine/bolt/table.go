package bolt

import (
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	bolt "github.com/etcd-io/bbolt"
)

type Table struct {
	Bucket *bolt.Bucket
	codec  record.Codec
}

func (t *Table) Insert(r record.Record) (rowid []byte, err error) {
	if !t.Bucket.Writable() {
		return nil, engine.ErrTransactionReadOnly
	}

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

	data, err := t.codec.Encode(r)
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

	return t.codec.Decode(v)
}

func (t *Table) Delete(rowid []byte) error {
	if !t.Bucket.Writable() {
		return engine.ErrTransactionReadOnly
	}

	v := t.Bucket.Get(rowid)
	if v == nil {
		return table.ErrRecordNotFound
	}

	return t.Bucket.Delete(rowid)
}

func (t *Table) Iterate(fn func([]byte, record.Record) error) error {
	return t.Bucket.ForEach(func(k, v []byte) error {
		if v == nil {
			return nil
		}

		return fn(k, record.EncodedRecord(v))
	})
}
