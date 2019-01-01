package table

import (
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
	Cursor() Cursor
	Record(id []byte) (record.Record, error)
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

// RecordBuffer contains a list of records. It implements the Reader interface.
type RecordBuffer struct {
	records map[string]record.Record
	ids     [][]byte
	counter int64
}

// Add a record to the buffer.
func (rb *RecordBuffer) Add(r record.Record) {
	rb.counter++
	id := field.EncodeInt64(rb.counter)
	rb.ids = append(rb.ids, id)
	if rb.records == nil {
		rb.records = make(map[string]record.Record)
	}

	rb.records[string(id)] = r
}

// AddFrom copies all the records of t to the buffer.
func (rb *RecordBuffer) AddFrom(t Reader) error {
	c := t.Cursor()

	for c.Next() {
		if c.Err() != nil {
			return c.Err()
		}

		rb.Add(c.Record())
	}

	return nil
}

func (rb *RecordBuffer) Record(id []byte) (record.Record, error) {
	rec, ok := rb.records[string(id)]
	if !ok {
		return nil, errors.New("not found")
	}

	return rec, nil
}

// Cursor creates a Cursor that iterates over the slice of records.
func (rb *RecordBuffer) Cursor() Cursor {
	return &recordBufferCursor{buf: rb, i: -1}
}

type recordBufferCursor struct {
	i   int
	buf *RecordBuffer
}

func (c *recordBufferCursor) Next() bool {
	if c.i+1 >= len(c.buf.records) {
		return false
	}

	c.i++
	return true
}

func (c *recordBufferCursor) Record() record.Record {
	return c.buf.records[string(c.buf.ids[c.i])]
}

func (c *recordBufferCursor) Err() error {
	return nil
}
