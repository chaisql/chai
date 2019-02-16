package table

import (
	"bytes"
	"container/list"
	"errors"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
)

// A Table represents a group of records.
type Table interface {
	Reader
	Writer
}

type Reader interface {
	Iterate(func(record.Record) bool) error
	Record(rowid []byte) (record.Record, error)
}

type Writer interface {
	Insert(record.Record) (rowid []byte, err error)
}

// RecordBuffer contains a list of records. It implements the Table interface.
type RecordBuffer struct {
	list    *list.List
	counter int64
}

type item struct {
	r     record.Record
	rowid []byte
}

// Insert adds a record to the buffer.
func (rb *RecordBuffer) Insert(r record.Record) ([]byte, error) {
	if rb.list == nil {
		rb.list = list.New()
	}

	rb.counter++

	rowid := field.EncodeInt64(rb.counter)
	rb.list.PushBack(&item{r, rowid})

	return rowid, nil
}

// InsertFrom copies all the records of t to the buffer.
func (rb *RecordBuffer) InsertFrom(t Reader) error {
	if rb.list == nil {
		rb.list = list.New()
	}

	if buf, ok := t.(*RecordBuffer); ok {
		rb.list.PushBackList(buf.list)
		return nil
	}

	return t.Iterate(func(r record.Record) bool {
		rb.Insert(r)
		return true
	})
}

func (rb *RecordBuffer) Record(rowid []byte) (record.Record, error) {
	if rb.list == nil {
		return nil, errors.New("not found")
	}

	for elm := rb.list.Front(); elm != nil; elm = elm.Next() {
		it := elm.Value.(*item)
		if bytes.Equal(it.rowid, rowid) {
			return it.r, nil
		}
	}
	return nil, errors.New("not found")
}

func (rb *RecordBuffer) Iterate(fn func(record.Record) bool) error {
	if rb.list == nil {
		return nil
	}

	for elm := rb.list.Front(); elm != nil; elm = elm.Next() {
		it := elm.Value.(*item)
		if !fn(it.r) {
			break
		}
	}

	return nil
}
