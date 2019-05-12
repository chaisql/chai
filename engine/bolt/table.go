package bolt

import (
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	bolt "github.com/etcd-io/bbolt"
)

type Table struct {
	bucket *bolt.Bucket
	codec  record.Codec
	tx     *bolt.Tx
	name   []byte
}

func (t *Table) Insert(r record.Record) (rowid []byte, err error) {
	if !t.bucket.Writable() {
		return nil, engine.ErrTransactionReadOnly
	}

	if pker, ok := r.(table.Pker); ok {
		rowid, err = pker.Pk()
		if err != nil {
			return nil, err
		}
	} else {
		seq, err := t.bucket.NextSequence()
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

	err = t.bucket.Put(rowid, data)
	if err != nil {
		return nil, err
	}

	return rowid, nil
}

func (t *Table) Record(rowid []byte) (record.Record, error) {
	v := t.bucket.Get(rowid)
	if v == nil {
		return nil, table.ErrRecordNotFound
	}

	return t.codec.Decode(v)
}

func (t *Table) Delete(rowid []byte) error {
	if !t.bucket.Writable() {
		return engine.ErrTransactionReadOnly
	}

	v := t.bucket.Get(rowid)
	if v == nil {
		return table.ErrRecordNotFound
	}

	return t.bucket.Delete(rowid)
}

func (t *Table) Iterate(fn func([]byte, record.Record) error) error {
	return t.bucket.ForEach(func(k, v []byte) error {
		if v == nil {
			return nil
		}

		r, err := t.codec.Decode(v)
		if err != nil {
			return err
		}

		return fn(k, r)
	})
}

func (t *Table) Replace(rowid []byte, r record.Record) error {
	if !t.bucket.Writable() {
		return engine.ErrTransactionReadOnly
	}

	v := t.bucket.Get(rowid)
	if v == nil {
		return table.ErrRecordNotFound
	}

	v, err := t.codec.Encode(r)
	if err != nil {
		return err
	}

	return t.bucket.Put(rowid, v)
}

func (t *Table) Truncate() error {
	if !t.bucket.Writable() {
		return engine.ErrTransactionReadOnly
	}

	err := t.tx.DeleteBucket(t.name)
	if err != nil {
		return err
	}

	_, err = t.tx.CreateBucket(t.name)
	return err
}
