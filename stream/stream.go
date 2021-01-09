package stream

import (
	"errors"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query"
	"github.com/genjidb/genji/sql/query/expr"
)

// ErrStreamClosed is used to indicate that a stream must be closed.
var ErrStreamClosed = errors.New("stream closed")

// Stream reads values from an iterator one by one and passes them
// through a list of operators for transformation.
// Streams can be cached and reused between transactions, they don't hold any state.
type Stream struct {
	It Iterator
	Op Operator
}

// New creates a stream using the given iterator.
func New(it Iterator) Stream {
	return Stream{
		It: it,
	}
}

// A Piper is an operator that can pipe itself into a stream.
type Piper interface {
	Pipe(Stream) Stream
}

// Pipe creates a new Stream who can read its data from s and apply
// op to every value passed by its Iterate method.
// If op implements the Piper interface, it will call its Pipe method instead.
func (s Stream) Pipe(op Operator) Stream {
	if p, ok := op.(Piper); ok {
		return p.Pipe(s)
	}

	return Stream{
		It: s,
		Op: op,
	}
}

// Iterate calls the underlying iterator's iterate method.
// If this stream was created using the Pipe method, it will apply fn
// to any value passed by the underlying iterator.
// If fn returns an error, the stream will be interrupted and that error will bubble up
// and returned by fn, unless that error is ErrStreamClosed, in which case
// the Iterate method will stop the iteration and return nil.
// It implements the Iterator interface.
func (s Stream) Iterate(env *expr.Environment, fn func(env *expr.Environment) error) error {
	if s.It == nil {
		return nil
	}

	if s.Op == nil {
		return s.It.Iterate(env, fn)
	}

	opFn, err := s.Op.Op()
	if err != nil {
		return err
	}

	err = s.It.Iterate(env, func(env *expr.Environment) error {
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

// Statement is a query.Statement using a Stream.
type Statement struct {
	Stream   Stream
	ReadOnly bool
}

// Run returns a result containing the stream. The stream will be executed by calling the Iterate method of
// the result.
func (s Statement) Run(tx *database.Transaction, params []expr.Param) (query.Result, error) {
	env := expr.Environment{
		Tx:     tx,
		Params: params,
	}

	return query.Result{
		Iterator: document.IteratorFunc(func(fn func(d document.Document) error) error {
			return s.Stream.Iterate(&env, func(env *expr.Environment) error {
				d, ok := env.GetDocument()
				if !ok {
					return nil
				}

				return fn(d)
			})
		}),
	}, nil
}

func (s Statement) IsReadOnly() bool {
	return s.ReadOnly
}
