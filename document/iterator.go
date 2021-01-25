package document

import (
	"bufio"
	"errors"
	"io"
)

// ErrStreamClosed is used to indicate that a stream must be closed.
var ErrStreamClosed = errors.New("stream closed")

// An Iterator can iterate over documents.
type Iterator interface {
	// Iterate goes through all the documents and calls the given function by passing each one of them.
	// If the given function returns an error, the iteration stops.
	Iterate(fn func(d Document) error) error
}

// NewIterator creates an iterator that iterates over documents.
func NewIterator(documents ...Document) Iterator {
	return documentsIterator(documents)
}

type documentsIterator []Document

func (rr documentsIterator) Iterate(fn func(d Document) error) error {
	var err error

	for _, d := range rr {
		err = fn(d)
		if err != nil {
			return err
		}
	}

	return nil
}

// The IteratorFunc type is an adapter to allow the use of ordinary functions as Iterators.
// If f is a function with the appropriate signature, IteratorFunc(f) is an Iterator that calls f.
type IteratorFunc func(fn func(d Document) error) error

// Iterate calls f(fn).
func (f IteratorFunc) Iterate(fn func(d Document) error) error {
	return f(fn)
}

// IteratorToJSON encodes all the documents of an iterator to JSON stream.
func IteratorToJSON(w io.Writer, s Iterator) error {
	buf := bufio.NewWriter(w)
	defer buf.Flush()

	return s.Iterate(func(d Document) error {
		data, err := jsonDocument{d}.MarshalJSON()
		if err != nil {
			return err
		}

		_, err = buf.Write(data)
		return err
	})
}

// IteratorToJSONArray encodes all the documents of an iterator to a JSON array.
func IteratorToJSONArray(w io.Writer, s Iterator) error {
	buf := bufio.NewWriter(w)

	buf.WriteByte('[')

	first := true
	err := s.Iterate(func(d Document) error {
		if !first {
			buf.WriteString(", ")
		} else {
			first = false
		}

		data, err := jsonDocument{d}.MarshalJSON()
		if err != nil {
			return err
		}

		_, err = buf.Write(data)
		return err
	})
	if err != nil {
		return err
	}

	buf.WriteByte(']')
	return buf.Flush()
}
