package memory

import (
	"bytes"
	"errors"

	"github.com/asdine/genji/engine"
	idx "github.com/asdine/genji/index"
	"github.com/google/btree"
)

type index struct {
	tree *btree.BTree
	tx   *transaction
}

type indexedItem struct {
	value, rowid []byte
}

func (i *indexedItem) Less(than btree.Item) bool {
	other := than.(*indexedItem)

	cmp := bytes.Compare(i.value, other.value)
	if cmp != 0 {
		return cmp < 0
	}

	if len(i.rowid) == 0 {
		return true
	}

	if len(other.rowid) == 0 {
		return false
	}

	return bytes.Compare(i.rowid, other.rowid) < 0
}

func (i *index) Set(value []byte, rowid []byte) error {
	if !i.tx.writable {
		return engine.ErrTransactionReadOnly
	}

	if len(value) == 0 {
		return errors.New("value cannot be nil")
	}

	i.tree.ReplaceOrInsert(&indexedItem{value, rowid})
	return nil
}

func (i *index) Cursor() idx.Cursor {
	return &indexCursor{
		tree: i.tree,
	}
}

type indexCursor struct {
	tree  *btree.BTree
	pivot btree.Item
}

func (c *indexCursor) First() ([]byte, []byte) {
	c.pivot = c.tree.Min()
	if c.pivot == nil {
		return nil, nil
	}

	it := c.pivot.(*indexedItem)
	return it.value, it.rowid
}

func (c *indexCursor) Last() ([]byte, []byte) {
	c.pivot = c.tree.Max()
	if c.pivot == nil {
		return nil, nil
	}

	it := c.pivot.(*indexedItem)
	return it.value, it.rowid
}

func (c *indexCursor) Next() ([]byte, []byte) {
	prev := c.pivot
	c.tree.AscendGreaterOrEqual(c.pivot, func(i btree.Item) bool {
		if c.pivot == i {
			return true
		}

		c.pivot = i
		return false
	})

	if c.pivot == prev {
		return nil, nil
	}

	it := c.pivot.(*indexedItem)
	return it.value, it.rowid
}

func (c *indexCursor) Prev() ([]byte, []byte) {
	prev := c.pivot
	c.tree.DescendLessOrEqual(c.pivot, func(i btree.Item) bool {
		if c.pivot == i {
			return true
		}

		c.pivot = i
		return false
	})

	if c.pivot == prev {
		return nil, nil
	}

	it := c.pivot.(*indexedItem)
	return it.value, it.rowid
}

func (c *indexCursor) Seek(seek []byte) ([]byte, []byte) {
	c.pivot = nil

	var k, v []byte

	c.tree.AscendGreaterOrEqual(&indexedItem{value: seek}, func(i btree.Item) bool {
		it := i.(*indexedItem)
		k, v = it.value, it.rowid

		c.pivot = i
		return false
	})

	return k, v
}
