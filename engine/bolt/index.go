package bolt

import (
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
	b, err := i.b.CreateBucketIfNotExists(value)
	if err != nil {
		return err
	}

	return b.Put(rowid, nil)
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
	ic  *bolt.Cursor
	val []byte
}

func (c *Cursor) First() ([]byte, []byte) {
	c.ic = nil
	val, _ := c.c.First()
	if val == nil {
		return nil, nil
	}

	c.val = val
	b := c.b.Bucket(val)
	if b == nil {
		return nil, nil
	}

	c.ic = b.Cursor()
	rowid, _ := c.ic.First()
	return val, rowid
}

func (c *Cursor) Last() ([]byte, []byte) {
	c.ic = nil
	val, _ := c.c.Last()
	if val == nil {
		return nil, nil
	}

	c.val = val
	b := c.b.Bucket(val)
	if b == nil {
		return nil, nil
	}

	c.ic = b.Cursor()
	rowid, _ := c.ic.Last()
	return val, rowid
}

func (c *Cursor) Next() ([]byte, []byte) {
	var val, rowid []byte

	if c.ic == nil {
		val, _ = c.c.Next()
		if val == nil {
			return nil, nil
		}

		c.val = val
		b := c.b.Bucket(val)
		if b == nil {
			return nil, nil
		}
		c.ic = b.Cursor()
		rowid, _ = c.ic.First()
	} else {
		rowid, _ = c.ic.Next()
		val = c.val
	}

	if rowid == nil {
		c.ic = nil
		return c.Next()
	}

	return val, rowid
}

func (c *Cursor) Prev() ([]byte, []byte) {
	var val, rowid []byte

	if c.ic == nil {
		val, _ = c.c.Prev()
		if val == nil {
			return nil, nil
		}

		c.val = val
		b := c.b.Bucket(val)
		if b == nil {
			return nil, nil
		}
		c.ic = b.Cursor()
		rowid, _ = c.ic.Last()
	} else {
		rowid, _ = c.ic.Prev()
		val = c.val
	}

	if rowid == nil {
		c.ic = nil
		return c.Prev()
	}

	return val, rowid
}

func (c *Cursor) Seek(seek []byte) ([]byte, []byte) {
	val, _ := c.c.Seek(seek)
	if val == nil {
		return nil, nil
	}

	c.val = val
	b := c.b.Bucket(val)
	if b == nil {
		return nil, nil
	}
	c.ic = b.Cursor()
	rowid, _ := c.ic.First()
	if rowid == nil {
		return nil, nil
	}

	return val, rowid
}
