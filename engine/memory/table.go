package memory

import (
	"errors"
	"io"
	"sync/atomic"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	"modernc.org/b"
)

type tableTx struct {
	table    string
	tx       *transaction
	writable bool
	tree     *b.Tree
	xid      uint64
	counter  uint64
}

type item struct {
	rootTree *b.Tree
	record   record.Record
	rowid    []byte
	xmin     uint64
}

func (t *tableTx) Record(rowid []byte) (record.Record, error) {
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

func (t *tableTx) Insert(r record.Record) (rowid []byte, err error) {
	if !t.writable {
		return nil, errors.New("can't insert record in read-only transaction")
	}

	rid := field.EncodeInt64(int64(atomic.AddUint64(&t.counter, 1)))

	it := item{
		rootTree: t.tree,
		record:   r,
		xmin:     t.xid,
		rowid:    rid,
	}

	t.tx.mutations = append(t.tx.mutations, &it)
	t.tree.Set(rid, &it)

	return rid, nil
}

func (t *tableTx) Cursor() table.Cursor {
	enum, err := t.tree.SeekFirst()

	return &cursor{
		err:  err,
		xid:  t.xid,
		enum: enum,
	}
}

type cursor struct {
	xid      uint64
	enum     *b.Enumerator
	curRowid []byte
	curItem  *item
	err      error
}

func (c *cursor) Next() bool {
	if c.err != nil {
		return false
	}

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
