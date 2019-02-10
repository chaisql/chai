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
		return nil, errors.New("not found")
	}

	return &record.EncodedRecord{Data: v}, nil
}

func (t *Table) Cursor() table.Cursor {
	return &tableCursor{
		b: t.Bucket,
	}
}

type tableCursor struct {
	b           *bolt.Bucket
	c           *bolt.Cursor
	rowid, data []byte
}

func (c *tableCursor) Next() bool {
	if c.c == nil {
		c.c = c.b.Cursor()
		c.rowid, c.data = c.c.First()
	} else {
		c.rowid, c.data = c.c.Next()
	}

	return c.rowid != nil
}

func (c *tableCursor) Err() error {
	return nil
}

func (c *tableCursor) Record() record.Record {
	return &record.EncodedRecord{Data: c.data}
}
