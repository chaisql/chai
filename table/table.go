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
	Cursor() Cursor
	Record(rowid []byte) (record.Record, error)
}

type Writer interface {
	Insert(record.Record) (rowid []byte, err error)
}

// A Cursor iterates over the fields of a record.
type Cursor interface {
	// Next advances the cursor to the next record which will then be available
	// through the Record method. It returns false when the cursor stops.
	// If an error occurs during iteration, the Err method will return it.
	Next() bool

	// Err returns the error, if any, that was encountered during iteration.
	Err() error

	// Record returns the current record.
	Record() record.Record
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

	c := t.Cursor()

	for c.Next() {
		if c.Err() != nil {
			return c.Err()
		}

		rb.Insert(c.Record())
	}

	return nil
}

func (rb *RecordBuffer) Record(rowid []byte) (record.Record, error) {
	return nil, nil
}

// Cursor creates a Cursor that iterates over the slice of records.
func (rb *RecordBuffer) Cursor() Cursor {
	return &recordBufferCursor{buf: rb}
}

type recordBufferCursor struct {
	buf *RecordBuffer
	cur *list.Element
}

func (c *recordBufferCursor) Next() bool {
	if c.cur == nil {
		c.cur = c.buf.list.Front()
		return c.cur != nil
	}

	next := c.cur.Next()
	if next == nil {
		return false
	}

	c.cur = next
	return true
}

func (c *recordBufferCursor) Record() record.Record {
	return c.cur.Value.(record.Record)
}

func (c *recordBufferCursor) Err() error {
	return nil
}
