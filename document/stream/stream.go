package stream

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
)

// ErrStreamClosed is used to indicate that a stream must be closed.
var ErrStreamClosed = errors.New("stream closed")

const (
	groupEnvKey = "_group"
	accEnvKey   = "_acc"
)

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
	Op() (OperatorFunc, error)
}

type Piper interface {
	Pipe(Stream) Stream
}

type OperatorFunc func(env *expr.Environment) (*expr.Environment, error)

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

// A MapOperator applies an expression on each value of the stream and returns a new value.
type MapOperator struct {
	E expr.Expr
}

// Map creates a MapOperator.
func Map(e expr.Expr) *MapOperator {
	return &MapOperator{E: e}
}

// Op implements the Operator interface.
func (m *MapOperator) Op() (OperatorFunc, error) {
	var newEnv expr.Environment

	return func(env *expr.Environment) (*expr.Environment, error) {
		v, err := m.E.Eval(env)
		if err != nil {
			return nil, err
		}

		newEnv.SetCurrentValue(v)
		newEnv.Outer = env
		return &newEnv, nil
	}, nil
}

func (m *MapOperator) String() string {
	return fmt.Sprintf("map(%s)", m.E)
}

// A FilterOperator applies an expression on each value of the stream and returns a new value.
type FilterOperator struct {
	E expr.Expr
}

// Filter creates a FilterOperator.
func Filter(e expr.Expr) *FilterOperator {
	return &FilterOperator{E: e}
}

// Op implements the Operator interface.
func (m *FilterOperator) Op() (OperatorFunc, error) {
	return func(env *expr.Environment) (*expr.Environment, error) {
		v, err := m.E.Eval(env)
		if err != nil {
			return nil, err
		}

		ok, err := v.IsTruthy()
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, nil
		}

		return env, nil
	}, nil
}

func (m *FilterOperator) String() string {
	return fmt.Sprintf("filter(%s)", m.E)
}

// A TakeOperator applies an expression on each value of the stream and returns a new value.
type TakeOperator struct {
	E expr.Expr
}

// Take creates a TakeOperator.
func Take(e expr.Expr) *TakeOperator {
	return &TakeOperator{E: e}
}

// Op implements the Operator interface.
func (m *TakeOperator) Op() (OperatorFunc, error) {
	var n, count int64
	v, err := m.E.Eval(&expr.Environment{})
	if err != nil {
		return nil, err
	}
	if v.Type != document.IntegerValue {
		v, err = v.CastAsInteger()
		if err != nil {
			return nil, err
		}
	}
	n = v.V.(int64)

	return func(env *expr.Environment) (*expr.Environment, error) {
		if count < n {
			count++
			return env, nil
		}

		return nil, ErrStreamClosed
	}, nil
}

func (m *TakeOperator) String() string {
	return fmt.Sprintf("take(%s)", m.E)
}

// A SkipOperator applies an expression on each value of the stream and returns a new value.
type SkipOperator struct {
	E expr.Expr
}

// Skip creates a SkipOperator.
func Skip(e expr.Expr) *SkipOperator {
	return &SkipOperator{E: e}
}

// Op implements the Operator interface.
func (m *SkipOperator) Op() (OperatorFunc, error) {
	var n, skipped int64
	v, err := m.E.Eval(&expr.Environment{})
	if err != nil {
		return nil, err
	}
	if v.Type != document.IntegerValue {
		v, err = v.CastAsInteger()
		if err != nil {
			return nil, err
		}
	}
	n = v.V.(int64)

	return func(env *expr.Environment) (*expr.Environment, error) {
		if skipped < n {
			skipped++
			return nil, nil
		}

		return env, nil
	}, nil
}

func (m *SkipOperator) String() string {
	return fmt.Sprintf("skip(%s)", m.E)
}

// A GroupByOperator applies an expression on each value of the stream and returns a new value.
type GroupByOperator struct {
	E expr.Expr
}

// GroupBy creates a GroupByOperator.
func GroupBy(e expr.Expr) *GroupByOperator {
	return &GroupByOperator{E: e}
}

// Op implements the Operator interface.
func (op *GroupByOperator) Op() (OperatorFunc, error) {
	var newEnv expr.Environment

	return func(env *expr.Environment) (*expr.Environment, error) {
		v, err := op.E.Eval(env)
		if err != nil {
			return nil, err
		}

		newEnv.Set(groupEnvKey, v)
		newEnv.Outer = env
		return &newEnv, nil
	}, nil
}

func (op *GroupByOperator) String() string {
	return fmt.Sprintf("groupBy(%s)", op.E)
}

// A ReduceOperator applies an expression on each value of the stream and returns a new value.
type ReduceOperator struct {
	Seed, Accumulator expr.Expr
	Stream            Stream
}

// Reduce creates a ReduceOperator.
func Reduce(seed, accumulator expr.Expr) *ReduceOperator {
	return &ReduceOperator{Seed: seed, Accumulator: accumulator}
}

func (op *ReduceOperator) Pipe(s Stream) Stream {
	op.Stream = s

	return Stream{
		it: s,
		op: op,
	}
}

// Op implements the Operator interface.
func (op *ReduceOperator) Op() (OperatorFunc, error) {
	var newEnv expr.Environment

	seed, err := op.Seed.Eval(&newEnv)
	if err != nil {
		return nil, err
	}

	newEnv.Set(accEnvKey, seed)
	data, _ := json.MarshalIndent(newEnv, "", "  ")
	fmt.Println(string(data))

	err = op.Stream.Iterate(func(env *expr.Environment) error {
		newEnv.Outer = env
		v, err := op.Accumulator.Eval(&newEnv)
		if err != nil {
			return err
		}

		newEnv.Set(accEnvKey, v)
		data, _ := json.MarshalIndent(newEnv, "", "  ")
		fmt.Println(string(data))
		return nil
	})
	if err != nil {
		return nil, err
	}

	return func(env *expr.Environment) (*expr.Environment, error) {
		v, _ := newEnv.Get(accEnvKey)
		newEnv.SetCurrentValue(v)
		newEnv.Outer = env
		return &newEnv, nil
	}, nil
}

func (op *ReduceOperator) String() string {
	return fmt.Sprintf("reduce(%s, %s)", op.Seed, op.Accumulator)
}
