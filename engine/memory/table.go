package memory

import (
	"bytes"
	"errors"
	"sync/atomic"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/google/btree"
)

type tableTx struct {
	tx      *transaction
	tree    *btree.BTree
	counter uint64
}

type item struct {
	record record.Record
	rowid  []byte
}

func (i *item) Less(than btree.Item) bool {
	return bytes.Compare(i.rowid, than.(*item).rowid) < 0
}

func (t *tableTx) Record(rowid []byte) (record.Record, error) {
	v := t.tree.Get(&item{rowid: rowid})
	if v == nil {
		return nil, engine.ErrNotFound
	}

	return v.(*item).record, nil
}

func (t *tableTx) Insert(r record.Record) (rowid []byte, err error) {
	if !t.tx.writable {
		return nil, errors.New("can't insert record in read-only transaction")
	}

	rid := field.EncodeInt64(int64(atomic.AddUint64(&t.counter, 1)))

	it := item{
		record: r,
		rowid:  rid,
	}

	t.tx.undos = append(t.tx.undos, func() {
		t.tree.Delete(&it)
	})

	t.tree.ReplaceOrInsert(&it)

	return rid, nil
}

func (t *tableTx) Iterate(fn func(record.Record) bool) error {
	t.tree.Ascend(func(i btree.Item) bool {
		return fn(i.(*item).record)
	})

	return nil
}
