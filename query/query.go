package query

import (
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/record"
)

type Query struct {
	t   engine.TableReader
	err error
}

func (q *Query) ForEach(fn func(record.Record) error) error {
	if q.err != nil {
		return q.err
	}

	c := q.t.Cursor()

	for c.Next() {
		if err := c.Err(); err != nil {
			return err
		}

		r := c.Record()

		err := fn(r)
		if err != nil {
			return err
		}
	}

	return nil
}

func (q *Query) Filter(fn func(record.Record) (bool, error)) *Query {
	var rb RecordBuffer

	err := q.ForEach(func(r record.Record) error {
		ok, err := fn(r)
		if err != nil {
			return err
		}

		if ok {
			rb.Add(r)
		}

		return nil
	})

	if err != nil {
		q.err = err
		return q
	}

	return &Query{t: rb}
}

func (q *Query) Map(fn func(record.Record) (record.Record, error)) *Query {
	var rb RecordBuffer

	err := q.ForEach(func(r record.Record) error {
		r, err := fn(r)
		if err != nil {
			return err
		}

		rb.Add(r)

		return nil
	})

	if err != nil {
		q.err = err
		return q
	}

	return &Query{t: rb}
}

// RecordBuffer contains a list of records. It implements the engine.TableReader interface.
type RecordBuffer []record.Record

// Add a record to the buffer.
func (rb *RecordBuffer) Add(r record.Record) {
	*rb = append(*rb, r)
}

// Cursor creates a Cursor that iterate over the slice of records.
func (rb RecordBuffer) Cursor() engine.Cursor {
	return &recordBufferCursor{buf: rb, i: -1}
}

type recordBufferCursor struct {
	i   int
	buf RecordBuffer
}

func (c *recordBufferCursor) Next() bool {
	if c.i+1 >= len(c.buf) {
		return false
	}

	c.i++
	return true
}

func (c *recordBufferCursor) Record() record.Record {
	return c.buf[c.i]
}

func (c *recordBufferCursor) Err() error {
	return nil
}
