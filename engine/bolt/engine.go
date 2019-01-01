package bolt

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	bolt "github.com/etcd-io/bbolt"
)

type Table struct {
	bucket *bolt.Bucket
}

func (t *Table) Insert(r record.Record) ([]byte, error) {
	seq, err := t.bucket.NextSequence()
	if err != nil {
		return nil, err
	}

	// TODO(asdine): encode in uint64 if that makes sense.
	rowid := field.EncodeInt64(int64(seq))

	c := r.Cursor()
	for c.Next() {
		if c.Err() != nil {
			return nil, c.Err()
		}

		f := c.Field()

		k := []byte(fmt.Sprintf("%s-%s-%d", rowid, f.Name, f.Type))

		err = t.bucket.Put(k, f.Data)
		if err != nil {
			return nil, err
		}
	}

	if err != nil {
		return nil, err
	}

	return rowid, nil
}

func (t *Table) Record(rowid []byte) (record.Record, error) {
	prefix := append(rowid, '-')
	k, _ := t.bucket.Cursor().Seek(prefix)
	if k == nil {
		return nil, errors.New("not found")
	}

	return &rec{
		b:     t.bucket,
		rowid: rowid,
	}, nil
}

func (t *Table) Cursor() table.Cursor {
	return &tableCursor{
		b: t.bucket,
	}
}

type tableCursor struct {
	b        *bolt.Bucket
	c        *bolt.Cursor
	k, v     []byte
	curRowID []byte
}

func (c *tableCursor) Next() bool {
	if c.c == nil {
		c.c = c.b.Cursor()
		c.k, c.v = c.c.First()
	} else {
		c.k, c.v = c.c.Next()
	}

	if c.k == nil {
		return false
	}

	if c.curRowID == nil {
		c.curRowID = c.k[0:bytes.IndexByte(c.k, '-')]
		return true
	}

	rowid := c.curRowID
	for ; c.k != nil && bytes.Equal(rowid, c.curRowID); c.k, c.v = c.c.Next() {
		rowid = c.k[0:bytes.IndexByte(c.k, '-')]
	}

	if c.k == nil {
		return false
	}

	c.curRowID = rowid
	return true
}

func (c *tableCursor) Err() error {
	return nil
}

func (c *tableCursor) Record() record.Record {
	return &rec{
		b:     c.b,
		rowid: c.curRowID,
	}
}

type rec struct {
	b     *bolt.Bucket
	rowid []byte
}

func (r *rec) get(name string) ([]byte, []byte, error) {
	prefix := []byte(fmt.Sprintf("%s-%s", r.rowid, name))
	k, v := r.b.Cursor().Seek(prefix)
	if v == nil || k == nil || !bytes.HasPrefix(k, prefix) {
		return nil, nil, errors.New("not found")
	}

	return k, v, nil
}

func (r *rec) Field(name string) (field.Field, error) {
	k, v, err := r.get(name)
	if err != nil {
		return field.Field{}, err
	}

	rawType := k[bytes.LastIndexByte(r.rowid, '-'):]
	typ, err := strconv.Atoi(string(rawType))
	if err != nil {
		return field.Field{}, err
	}

	return field.Field{
		Name: name,
		Type: field.Type(typ),
		Data: v,
	}, nil
}

func (r *rec) Cursor() record.Cursor {
	return &recCursor{
		c:     r.b.Cursor(),
		rowid: r.rowid,
	}
}

type recCursor struct {
	c     *bolt.Cursor
	rowid []byte
	k, v  []byte
	err   error
}

func (r *recCursor) Next() bool {
	if r.k == nil {
		r.k, r.v = r.c.Seek(r.rowid)
	} else {
		r.k, r.v = r.c.Next()
	}

	if r.k == nil {
		return false
	}

	curRowID := r.k[0:bytes.IndexByte(r.k, '-')]

	return bytes.Equal(r.rowid, curRowID)
}

func (r *recCursor) Err() error {
	return r.err
}

func (r *recCursor) Field() field.Field {
	k := bytes.TrimPrefix(r.k, r.rowid)[1:]

	rawType := k[bytes.LastIndexByte(k, '-'):]
	typ, err := strconv.Atoi(string(rawType))
	if err != nil {
		r.err = err
		return field.Field{}
	}

	idx := bytes.IndexByte(k, '-')

	return field.Field{
		Name: string(k[0:idx]),
		Type: field.Type(typ),
		Data: r.v,
	}
}
