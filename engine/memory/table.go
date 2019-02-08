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
	counter  uint64
}

type item struct {
	rootTree *b.Tree
	record   record.Record
	rowid    []byte
}

func (t *tableTx) Record(rowid []byte) (record.Record, error) {
	v, ok := t.tree.Get(rowid)
	if !ok {
		return nil, errors.New("not found")
	}

	return v.(*item).record, nil
}

func (t *tableTx) Insert(r record.Record) (rowid []byte, err error) {
	if !t.writable {
		return nil, errors.New("can't insert record in read-only transaction")
	}

	rid := field.EncodeInt64(int64(atomic.AddUint64(&t.counter, 1)))

	it := item{
		rootTree: t.tree,
		record:   r,
		rowid:    rid,
	}

	t.tx.undos = append(t.tx.undos, func() {
		t.tree.Delete(rid)
	})

	t.tree.Set(rid, &it)

	return rid, nil
}

func (t *tableTx) Cursor() table.Cursor {
	enum, err := t.tree.SeekFirst()

	return &cursor{
		err:  err,
		enum: enum,
	}
}

type cursor struct {
	enum     *b.Enumerator
	curRowid []byte
	curItem  *item
	err      error
}

func (c *cursor) Next() bool {
	if c.err != nil {
		return false
	}

	k, v, err := c.enum.Next()
	if err == io.EOF {
		return false
	}

	if err != nil {
		c.err = err
		return false
	}

	c.curRowid = k.([]byte)
	c.curItem = v.(*item)
	return true
}

func (c *cursor) Err() error {
	return c.err
}

func (c *cursor) Record() record.Record {
	return c.curItem.record
}
