package memory

import (
	"bytes"
	"errors"
	"sync/atomic"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/google/btree"
)

type table struct {
	writable bool
	tree     *btree.BTree
	xid      uint64
	counter  uint64
}

type item struct {
	record record.Record
	rowid  []byte
	xmin   uint64
}

func (i *item) Less(than btree.Item) bool {
	return bytes.Compare(i.rowid, than.(*item).rowid) < 0
}

func (t *table) Record(rowid []byte) (record.Record, error) {
	i := t.tree.Get(&item{rowid: rowid})
	if i == nil {
		return nil, errors.New("not found")
	}

	it := i.(*item)
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

	t.tree.ReplaceOrInsert(&item{
		record: r,
		xmin:   t.xid,
		rowid:  rid,
	})

	return rid, nil
}
