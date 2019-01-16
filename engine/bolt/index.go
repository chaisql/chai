package bolt

import (
	"bytes"

	"github.com/asdine/genji/field"
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
	var counter int64 = 1
	var err error

	c := i.b.Cursor()
	v, _ := c.Seek(value)
	if bytes.HasPrefix(v, value) {
		for bytes.HasPrefix(v, value) {
			v, _ = c.Next()
		}
		if v != nil && !bytes.HasPrefix(v, value) {
			v, _ = c.Prev()
		} else if v == nil {
			v, _ = c.Last()
		}

		counter, err = field.DecodeInt64(v[bytes.LastIndexByte(v, '_')+1:])
		if err != nil {
			return err
		}
		counter++
	}

	buf := bytes.NewBuffer(value)
	_, err = buf.WriteRune('_')
	if err != nil {
		return err
	}

	_, err = buf.Write(field.EncodeInt64(counter))
	if err != nil {
		return err
	}

	return i.b.Put(buf.Bytes(), rowid)
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
