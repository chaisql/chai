package bolt

import (
	"bytes"
	"errors"

	"github.com/asdine/genji/engine"
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
	if len(value) == 0 {
		return errors.New("value cannot be nil")
	}

	buf := make([]byte, 0, len(value)+len(rowid)+1)
	buf = append(buf, value...)
	buf = append(buf, '_')
	buf = append(buf, rowid...)

	err := i.b.Put(buf, nil)
	if err == bolt.ErrTxNotWritable {
		return engine.ErrTransactionReadOnly
	}

	return err
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
	value, _ := c.c.First()
	if value == nil {
		return nil, nil
	}

	idx := bytes.LastIndexByte(value, '_')
	return value[:idx], value[idx+1:]
}

func (c *Cursor) Last() ([]byte, []byte) {
	value, _ := c.c.Last()
	if value == nil {
		return nil, nil
	}

	idx := bytes.LastIndexByte(value, '_')
	return value[:idx], value[idx+1:]
}

func (c *Cursor) Next() ([]byte, []byte) {
	value, _ := c.c.Next()
	if value == nil {
		c.c.Last()
		return nil, nil
	}

	idx := bytes.LastIndexByte(value, '_')
	return value[:idx], value[idx+1:]
}

func (c *Cursor) Prev() ([]byte, []byte) {
	value, _ := c.c.Prev()
	if value == nil {
		c.c.First()
		return nil, nil
	}

	idx := bytes.LastIndexByte(value, '_')
	return value[:idx], value[idx+1:]
}

func (c *Cursor) Seek(seek []byte) ([]byte, []byte) {
	value, _ := c.c.Seek(seek)
	if value == nil {
		return nil, nil
	}

	idx := bytes.LastIndexByte(value, '_')
	return value[:idx], value[idx+1:]
}
