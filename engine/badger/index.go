package badger

import (
	"bytes"
	"errors"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/index"
	"github.com/dgraph-io/badger"
)

type Index struct {
	txn      *badger.Txn
	prefix   []byte
	writable bool
}

func (i *Index) Set(value []byte, rowid []byte) error {
	if len(value) == 0 {
		return errors.New("value cannot be nil")
	}

	if !i.writable {
		return engine.ErrTransactionReadOnly
	}

	buf := make([]byte, 0, len(i.prefix)+1+len(value)+1+len(rowid))
	buf = append(buf, i.prefix...)
	buf = append(buf, separator)
	buf = append(buf, value...)
	buf = append(buf, separator)
	buf = append(buf, rowid...)

	return i.txn.Set(buf, rowid)
}

func (i *Index) Cursor() index.Cursor {
	return &Cursor{
		b: i.b,
		c: i.b.Cursor(),
	}
}

type Cursor struct {
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
		c.c.Last()
		return nil, nil
	}

	return value[:bytes.LastIndexByte(value, '_')], rowid
}

func (c *Cursor) Prev() ([]byte, []byte) {
	value, rowid := c.c.Prev()
	if value == nil {
		c.c.First()
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
