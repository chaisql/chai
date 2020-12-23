package stream

import (
	"errors"
	"fmt"

	"github.com/genjidb/genji/sql/query/expr"
)

// ErrStreamClosed is used to indicate that a stream must be closed.
var ErrStreamClosed = errors.New("stream closed")

// An Operator is used to modify a stream.
// It takes an environment containing the current value as well as any other metadata and returns
// a new environment which will be passed to the next operator.
// If it returns an environment with no value, the env will be ignored.
// If it returns an error, the stream will be interrupted and that error will bubble up
// and returned by this function, unless that error is ErrStreamClosed, in which case
// the Iterate method will stop the iteration and return nil.
// Stream operators can be reused, and thus, any state or side effect should be kept within the Op closure
// unless the nature of the operator prevents that.
type Operator interface {
	Op() func(env *expr.Environment) (*expr.Environment, error)
}

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

// Pipe creates a new Stream who can read its data from s and apply
// op to every value passed by its Iterate method.
func (s Stream) Pipe(op Operator) Stream {
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

	opFn := s.op.Op()

	err := s.it.Iterate(func(env *expr.Environment) error {
		env, err := opFn(env)
		if err != nil {
			return err
		}

		return fn(env)
	})
	if err != ErrStreamClosed {
		return err
	}

	return nil
}

// A MapOperator applies an expression on each value of the stream and returns a new value.
type MapOperator struct {
	E expr.Expr
}

// Map creates a MapOperator.
func Map(e expr.Expr) *MapOperator {
	return &MapOperator{E: e}
}

// Op implements the Operator interface.
func (m *MapOperator) Op() func(env *expr.Environment) (*expr.Environment, error) {
	var newEnv expr.Environment

	return func(env *expr.Environment) (*expr.Environment, error) {
		v, err := m.E.Eval(env)
		if err != nil {
			return nil, err
		}

		newEnv.SetCurrentValue(v)
		newEnv.Outer = env
		return &newEnv, nil
	}
}

func (m *MapOperator) String() string {
	return fmt.Sprintf("map(%s)", m.E)
}
