package memory

import (
	"errors"
	"io"
	"sync/atomic"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	tab "github.com/asdine/genji/table"
	"modernc.org/b"
)

type table struct {
	writable bool
	tree     *b.Tree
	xid      uint64
	counter  uint64
}

type item struct {
	record record.Record
	rowid  []byte
	xmin   uint64
}

func (t *table) Record(rowid []byte) (record.Record, error) {
	v, ok := t.tree.Get(rowid)
	if !ok {
		return nil, errors.New("not found")
	}

	it := v.(*item)
	if it.xmin > t.xid {
		return nil, errors.New("not found")
	}

	return it.record, nil
}

func (t *table) Insert(r record.Record) (rowid []byte, err error) {
	if !t.writable {
		return nil, errors.New("can't insert record in read-only transaction")
	}

	rid := field.EncodeInt64(int64(atomic.AddUint64(&t.counter, 1)))

	t.tree.Set(rid, &item{
		record: r,
		xmin:   t.xid,
		rowid:  rid,
	})

	return rid, nil
}

func (t *table) Cursor() (tab.Cursor, error) {
	enum, err := t.tree.SeekFirst()
	if err != nil {
		return nil, err
	}

	return &cursor{
		xid:  t.xid,
		enum: enum,
	}, nil
}

type cursor struct {
	xid      uint64
	enum     *b.Enumerator
	curRowid []byte
	curItem  *item
	err      error
}

func (c *cursor) Next() bool {
	for {
		k, v, err := c.enum.Next()
		if err == io.EOF {
			return false
		}

		if err != nil {
			c.err = err
			return false
		}

		it := v.(*item)
		if it.xmin < c.xid {
			c.curRowid = k.([]byte)
			c.curItem = it
			return true
		}
	}
}

func (c *cursor) Err() error {
	return c.err
}

func (c *cursor) Record() record.Record {
	return c.curItem.record
}
