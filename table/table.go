package table

import (
	"container/list"

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

// Insert adds a record to the buffer.
func (rb *RecordBuffer) Insert(r record.Record) ([]byte, error) {
	if rb.list == nil {
		rb.list = list.New()
	}

	rb.counter++

	rb.list.PushBack(r)

	return field.EncodeInt64(rb.counter), nil
}

// InsertFrom copies all the records of t to the buffer.
func (rb *RecordBuffer) InsertFrom(t Reader) error {
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
	return nil, nil
}

func (rb *RecordBuffer) Iterate(fn func(record.Record) bool) error {
	elm := rb.list.Front()
	if elm == nil {
		return nil
	}

	for elm := rb.list.Front(); elm != nil; elm = elm.Next() {
		if !fn(elm.Value.(record.Record)) {
			break
		}
	}

	return nil
}
