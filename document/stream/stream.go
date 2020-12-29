package stream

import (
	"errors"

	"github.com/genjidb/genji/sql/query/expr"
)

// ErrStreamClosed is used to indicate that a stream must be closed.
var ErrStreamClosed = errors.New("stream closed")

// Stream reads values from an iterator one by one and passes them
// through a list of operators for transformation.
type Stream struct {
	it Iterator
	op Operator
}

// New creates a stream using the given iterator.
func New(it Iterator) Stream {
	return Stream{
		it: it,
	}
}

// A Piper is an operator that can pipe itself into a stream.
type Piper interface {
	Pipe(Stream) Stream
}

// Pipe creates a new Stream who can read its data from s and apply
// op to every value passed by its Iterate method.
func (s Stream) Pipe(op Operator) Stream {
	if p, ok := op.(Piper); ok {
		return p.Pipe(s)
	}

	return Stream{
		it: s,
		op: op,
	}
}

// Iterate calls the underlying iterator's iterate method.
// If this stream was created using the Pipe method, it will apply fn
// to any value passed by the underlying iterator.
// If fn returns an error, the stream will be interrupted and that error will bubble up
// and returned by fn, unless that error is ErrStreamClosed, in which case
// the Iterate method will stop the iteration and return nil.
// It implements the Iterator interface.
func (s Stream) Iterate(fn func(env *expr.Environment) error) error {
	if s.it == nil {
		return nil
	}

	if s.op == nil {
		return s.it.Iterate(fn)
	}

	opFn, err := s.op.Op()
	if err != nil {
		return err
	}

	err = s.it.Iterate(func(env *expr.Environment) error {
		env, err := opFn(env)
		if err != nil {
			return err
		}
		if env == nil {
			return nil
		}

		return fn(env)
	})
	if err != ErrStreamClosed {
		return err
	}

	return nil
}
