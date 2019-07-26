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

func (i *Index) Set(value []byte, recordID []byte) error {
	if len(value) == 0 {
		return errors.New("value cannot be nil")
	}

	buf := make([]byte, 0, len(value)+len(recordID)+1)
	buf = append(buf, value...)
	buf = append(buf, separator)
	buf = append(buf, recordID...)

	err := i.b.Put(buf, nil)
	if err == bolt.ErrTxNotWritable {
		return engine.ErrTransactionReadOnly
	}

	return err
}

func (i *Index) Delete(recordID []byte) error {
	if !i.b.Writable() {
		return engine.ErrTransactionReadOnly
	}

	suffix := make([]byte, len(recordID)+1)
	suffix[0] = separator
	copy(suffix[1:], recordID)

	errStop := errors.New("stop")

	err := i.b.ForEach(func(k []byte, v []byte) error {
		if bytes.HasSuffix(k, suffix) {
			err := i.b.Delete(k)
			if err != nil {
				return err
			}
			return errStop
		}

		return nil
	})

	if err != errStop {
		return err
	}

	return nil
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

	idx := bytes.LastIndexByte(value, separator)
	return value[:idx], value[idx+1:]
}

func (c *Cursor) Last() ([]byte, []byte) {
	value, _ := c.c.Last()
	if value == nil {
		return nil, nil
	}

	idx := bytes.LastIndexByte(value, separator)
	return value[:idx], value[idx+1:]
}

func (c *Cursor) Next() ([]byte, []byte) {
	value, _ := c.c.Next()
	if value == nil {
		c.c.Last()
		return nil, nil
	}

	idx := bytes.LastIndexByte(value, separator)
	return value[:idx], value[idx+1:]
}

func (c *Cursor) Prev() ([]byte, []byte) {
	value, _ := c.c.Prev()
	if value == nil {
		c.c.First()
		return nil, nil
	}

	idx := bytes.LastIndexByte(value, separator)
	return value[:idx], value[idx+1:]
}

func (c *Cursor) Seek(seek []byte) ([]byte, []byte) {
	value, _ := c.c.Seek(seek)
	if value == nil {
		return nil, nil
	}

	idx := bytes.LastIndexByte(value, separator)
	return value[:idx], value[idx+1:]
}
