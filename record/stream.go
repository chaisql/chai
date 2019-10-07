package record

import (
	"bufio"
	"errors"
	"fmt"
	"io"
)

// ErrStreamClosed is used to indicate that a stream must be closed.
var ErrStreamClosed = errors.New("stream closed")

// An Iterator can iterate over records.
type Iterator interface {
	// Iterate goes through all the records and calls the given function by passing each one of them.
	// If the given function returns an error, the iteration stops.
	Iterate(func(r Record) error) error
}

// NewIterator creates an iterator that iterates over records.
func NewIterator(records ...Record) Iterator {
	return recordsIterator(records)
}

type recordsIterator []Record

func (rr recordsIterator) Iterate(fn func(r Record) error) error {
	var err error

	for _, r := range rr {
		err = fn(r)
		if err != nil {
			return err
		}
	}

	return nil
}

// Stream reads records of an iterator one by one and passes them
// through a list of functions for transformation.
type Stream struct {
	it Iterator
	op Operator
}

// NewStream creates a stream using the given iterator.
func NewStream(it Iterator) Stream {
	return Stream{it: it}
}

// Iterate calls the underlying iterator's iterate method.
// If this stream was created using the Pipe method, it will apply fn
// to any record passed by the underlying iterator.
// If fn returns a record, it will be passed to the next stream.
// If it returns a nil record, the record will be ignored.
// If it returns an error, the stream will be interrupted and that error will bubble up
// and returned by fn, unless that error is ErrStreamClosed, in which case
// the Iterate method will stop the iteration and return nil.
// It implements the Iterator interface.
func (s Stream) Iterate(fn func(r Record) error) error {
	if s.it == nil {
		return nil
	}

	if s.op == nil {
		return s.it.Iterate(fn)
	}

	opFn := s.op()

	err := s.it.Iterate(func(r Record) error {
		r, err := opFn(r)
		if err != nil {
			return err
		}

		if r == nil {
			return nil
		}

		return fn(r)
	})
	if err != ErrStreamClosed {
		return err
	}

	return nil
}

// Pipe creates a new Stream who can read its data from s and apply
// op to every record passed by its Iterate method.
func (s Stream) Pipe(op Operator) Stream {
	return Stream{
		it: s,
		op: op,
	}
}

// Map applies fn to each received record and passes it to the next stream.
// If fn returns an error, the stream is interrupted.
func (s Stream) Map(fn func(r Record) (Record, error)) Stream {
	return s.Pipe(func() func(r Record) (Record, error) {
		return fn
	})
}

// Filter each received record using fn.
// If fn returns true, the record is kept, otherwise it is skipped.
// If fn returns an error, the stream is interrupted.
func (s Stream) Filter(fn func(r Record) (bool, error)) Stream {
	return s.Pipe(func() func(r Record) (Record, error) {
		return func(r Record) (Record, error) {
			ok, err := fn(r)
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
	return s.Pipe(func() func(r Record) (Record, error) {
		var count int

		return func(r Record) (Record, error) {
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
	return s.Pipe(func() func(r Record) (Record, error) {
		var skipped int

		return func(r Record) (Record, error) {
			if skipped < n {
				skipped++
				return nil, nil
			}

			return r, nil
		}
	})
}

// Append adds the given iterator to the stream.
func (s Stream) Append(it Iterator) Stream {
	if mr, ok := s.it.(multiIterator); ok {
		mr.iterators = append(mr.iterators, it)
		s.it = mr
	} else {
		s.it = multiIterator{
			iterators: []Iterator{s, it},
		}
	}

	return s
}

// Count counts all the records from the stream.
func (s Stream) Count() (int, error) {
	counter := 0

	err := s.Iterate(func(r Record) error {
		counter++
		return nil
	})

	return counter, err
}

// First runs the stream, returns the first record found and closes the stream.
// If the stream is empty, all return values are nil.
func (s Stream) First() (r Record, err error) {
	err = s.Iterate(func(rec Record) error {
		r = rec
		return ErrStreamClosed
	})

	if err == ErrStreamClosed {
		err = nil
	}

	return
}

// Dump stream information to w, structured as a csv.
func (s Stream) Dump(w io.Writer) error {
	buf := bufio.NewWriter(w)

	err := s.Iterate(func(r Record) error {
		first := true
		err := r.Iterate(func(f Field) error {
			if !first {
				buf.WriteString(", ")
			}
			first = false

			v, err := f.Decode()

			fmt.Fprintf(buf, "%s(%s): %#v", f.Name, f.Type, v)
			return err
		})
		if err != nil {
			return err
		}

		fmt.Fprintf(buf, "\n")
		return nil
	})
	if err != nil {
		return err
	}

	return buf.Flush()
}

// An Operator is used to modify a stream.
// If an operator returns a record, it will be passed to the next stream.
// If it returns a nil record, the record will be ignored.
// If it returns an error, the stream will be interrupted and that error will bubble up
// and returned by this function, unless that error is ErrStreamClosed, in which case
// the Iterate method will stop the iteration and return nil.
// Operators can be reused, and thus, any side effect should be kept within the operator closure
// unless the nature of the operator prevents that.
type Operator func() func(r Record) (Record, error)

type multiIterator struct {
	iterators []Iterator
}

func (m multiIterator) Iterate(fn func(r Record) error) error {
	for _, it := range m.iterators {
		err := it.Iterate(fn)
		if err != nil {
			return err
		}
	}

	return nil
}
