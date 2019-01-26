package bolt

import (
	"bytes"

	"github.com/asdine/genji/index"
	bolt "github.com/etcd-io/bbolt"
)

type Index struct {
	b *bolt.Bucket
}

func NewIndex(b *bolt.Bucket) *Index {
	return &Index{b}
}

func (i *Index) Set(value []byte, rowid []byte) error {
	buf := make([]byte, 0, len(value)+len(rowid)+1)
	buf = append(buf, value...)
	buf = append(buf, '_')
	buf = append(buf, rowid...)

	return i.b.Put(buf, rowid)
}

func (i *Index) Cursor() index.Cursor {
	return &Cursor{
		b: i.b,
		c: i.b.Cursor(),
	}
}

type Cursor struct {
	b   *bolt.Bucket
	c   *bolt.Cursor
	val []byte
}

func (c *Cursor) First() ([]byte, []byte) {
	value, rowid := c.c.First()
	if value == nil {
		return nil, nil
	}

	return value[:bytes.LastIndexByte(value, '_')], rowid
}

func (c *Cursor) Last() ([]byte, []byte) {
	value, rowid := c.c.Last()
	if value == nil {
		return nil, nil
	}

	return value[:bytes.LastIndexByte(value, '_')], rowid
}

func (c *Cursor) Next() ([]byte, []byte) {
	value, rowid := c.c.Next()
	if value == nil {
		return nil, nil
	}

	return value[:bytes.LastIndexByte(value, '_')], rowid
}

func (c *Cursor) Prev() ([]byte, []byte) {
	value, rowid := c.c.Prev()
	if value == nil {
		return nil, nil
	}

	return value[:bytes.LastIndexByte(value, '_')], rowid
}

func (c *Cursor) Seek(seek []byte) ([]byte, []byte) {
	value, rowid := c.c.Seek(seek)
	if value == nil {
		return nil, nil
	}

	return value[:bytes.LastIndexByte(value, '_')], rowid
}
