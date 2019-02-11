package memory

import (
	"bytes"

	"github.com/asdine/genji/index"
	"modernc.org/b"
)

type Index struct {
	tree *b.Tree
}

func NewIndex() *Index {
	return &Index{
		tree: b.TreeNew(func(a, b interface{}) int {
			ita, itb := a.(*indexedItem), b.(*indexedItem)

			cmp := bytes.Compare(ita.value, itb.value)
			if cmp != 0 {
				return cmp
			}

			return bytes.Compare(ita.rowid, itb.rowid)
		}),
	}
}

type indexedItem struct {
	value, rowid []byte
}

func (i *Index) Set(value []byte, rowid []byte) error {
	i.tree.Set(&indexedItem{value, rowid}, rowid)
	return nil
}

func (i *Index) Cursor() index.Cursor {
	return &indexCursor{
		tree: i.tree,
	}
}

type indexCursor struct {
	tree *b.Tree
	enum *b.Enumerator
}

func (c *indexCursor) First() ([]byte, []byte) {
	var err error
	c.enum, err = c.tree.SeekFirst()

	if err != nil {
		return nil, nil
	}

	return c.Next()
}

func (c *indexCursor) Last() ([]byte, []byte) {
	var err error
	c.enum, err = c.tree.SeekLast()

	if err != nil {
		return nil, nil
	}

	return c.Prev()
}

func (c *indexCursor) Next() ([]byte, []byte) {
	k, _, err := c.enum.Next()
	if err != nil {
		c.Last()
		return nil, nil
	}

	it := k.(*indexedItem)
	return it.value, it.rowid
}

func (c *indexCursor) Prev() ([]byte, []byte) {
	k, _, err := c.enum.Prev()
	if err != nil {
		c.First()
		return nil, nil
	}

	it := k.(*indexedItem)
	return it.value, it.rowid
}

func (c *indexCursor) Seek(seek []byte) ([]byte, []byte) {
	c.enum, _ = c.tree.Seek(&indexedItem{value: seek})

	return c.Next()
}
