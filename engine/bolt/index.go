package bolt

import (
	"fmt"

	"github.com/asdine/genji/index"
	bolt "github.com/etcd-io/bbolt"
)

type Index struct {
	b *bolt.Bucket
}

func (i *Index) Set(d []byte, rowid []byte) error {
	b, err := i.b.CreateBucketIfNotExists(d)
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

func (c *Cursor) First() ([]byte, []byte, error) {
	c.ic = nil
	val, _ := c.c.First()
	if val == nil {
		return nil, nil, nil
	}

	c.val = val
	b := c.b.Bucket(val)
	if b == nil {
		return nil, nil, fmt.Errorf("unknown bucket %s", val)
	}

	c.ic = b.Cursor()
	rowid, _ := c.ic.First()
	return val, rowid, nil
}

func (c *Cursor) Last() ([]byte, []byte, error) {
	c.ic = nil
	val, _ := c.c.Last()
	if val == nil {
		return nil, nil, nil
	}

	c.val = val
	b := c.b.Bucket(val)
	if b == nil {
		return nil, nil, fmt.Errorf("unknown bucket %s", val)
	}

	c.ic = b.Cursor()
	rowid, _ := c.ic.Last()
	return val, rowid, nil
}

func (c *Cursor) Next() ([]byte, []byte, error) {
	var val, rowid []byte

	if c.ic == nil {
		val, _ = c.c.Next()
		if val == nil {
			return nil, nil, nil
		}

		c.val = val
		b := c.b.Bucket(val)
		if b == nil {
			return nil, nil, fmt.Errorf("unknown bucket %s", val)
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

	return val, rowid, nil
}

func (c *Cursor) Prev() ([]byte, []byte, error) {
	var val, rowid []byte

	if c.ic == nil {
		val, _ = c.c.Prev()
		if val == nil {
			return nil, nil, nil
		}

		c.val = val
		b := c.b.Bucket(val)
		if b == nil {
			return nil, nil, fmt.Errorf("unknown bucket %s", val)
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

	return val, rowid, nil
}
