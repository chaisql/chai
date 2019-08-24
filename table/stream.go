package table

import (
	"errors"

	"github.com/asdine/genji/record"
)

// ErrStreamClosed is used to indicate that a stream must be closed.
var ErrStreamClosed = errors.New("stream closed")

// Stream reads records of a table reader one by one and passes them
// through a list of functions for transformation.
type Stream struct {
	rd Reader
	op Operator
}

// NewStream creates a stream using the given reader.
func NewStream(rd Reader) Stream {
	return Stream{rd: rd}
}

// Iterate calls the underlying reader iterate method.
// If this stream was created using the Pipe method, it will apply the given operation
// to any record passed by the underlying reader.
// If an operator returns a record, it will be passed to the next stream.
// If it returns a nil record, the record will be ignored.
// If it returns an error, the stream will be interrupted and that error will bubble up
// and returned by this function, unless that error is ErrStreamClosed, in which case
// the Iterate method will stop the iteration and return nil.
// It implements the Reader interface.
func (s Stream) Iterate(fn func(recordID []byte, r record.Record) error) error {
	if s.op == nil {
		return s.rd.Iterate(fn)
	}

	opFn := s.op()

	err := s.rd.Iterate(func(recordID []byte, r record.Record) error {
		r, err := opFn(recordID, r)
		if err != nil {
			return err
		}

		if r == nil {
			return nil
		}

		return fn(recordID, r)
	})
	if err != ErrStreamClosed {
		return err
	}

	return nil
}

// Pipe creates a new Stream who can read its data from s and apply
// the given operator to every record passed by its Iterate method.
func (s Stream) Pipe(op Operator) Stream {
	return Stream{
		rd: s,
		op: op,
	}
}

// Map applies fn to each received record and passes it to the next stream.
// If fn returns an error, the stream is interrupted.
func (s Stream) Map(fn func(recordID []byte, r record.Record) (record.Record, error)) Stream {
	return s.Pipe(func() func(recordID []byte, r record.Record) (record.Record, error) {
		return fn
	})
}

// Filter filters each received record using fn.
// If fn returns true, the record is kept, otherwise it is skipped.
// If fn returns an error, the stream is interrupted.
func (s Stream) Filter(fn func(recordID []byte, r record.Record) (bool, error)) Stream {
	return s.Pipe(func() func(recordID []byte, r record.Record) (record.Record, error) {
		return func(recordID []byte, r record.Record) (record.Record, error) {
			ok, err := fn(recordID, r)
			if err != nil {
				return nil, err
			}

			if !ok {
				return nil, nil
			}

			return r, nil
		}
	})
}

// Limit interrupts the stream once the number of passed records have reached n.
func (s Stream) Limit(n int) Stream {
	return s.Pipe(func() func(recordID []byte, r record.Record) (record.Record, error) {
		var count int

		return func(recordID []byte, r record.Record) (record.Record, error) {
			if count < n {
				count++
				return r, nil
			}

			return nil, ErrStreamClosed
		}
	})
}

// Offset ignores n records then passes the subsequent ones to the stream.
func (s Stream) Offset(n int) Stream {
	return s.Pipe(func() func(recordID []byte, r record.Record) (record.Record, error) {
		var skipped int

		return func(recordID []byte, r record.Record) (record.Record, error) {
			if skipped < n {
				skipped++
				return nil, nil
			}

			return r, nil
		}
	})
}

// Count counts all the records from the stream.
func (s Stream) Count() (int, error) {
	counter := 0

	err := s.Iterate(func(recordID []byte, r record.Record) error {
		counter++
		return nil
	})

	return counter, err
}

// First runs the stream, returns the first record found and closes the stream.
// If the stream is empty, all return values are nil.
func (s Stream) First() (recordID []byte, r record.Record, err error) {
	err = s.Limit(1).Iterate(func(rID []byte, rec record.Record) error {
		recordID = rID
		r = rec
		return nil
	})

	return
}

// An Operator is used to modify a stream.
// If an operator returns a record, it will be passed to the next stream.
// If it returns a nil record, the record will be ignored.
// If it returns an error, the stream will be interrupted and that error will bubble up
// and returned by this function, unless that error is ErrStreamClosed, in which case
// the Iterate method will stop the iteration and return nil.
// Operators can be reused, and thus, any side effect should be kept within the operator closure
// unless the nature of the operator prevents that.
type Operator func() func(recordID []byte, r record.Record) (record.Record, error)
