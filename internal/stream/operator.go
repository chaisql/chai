package stream

import (
	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/environment"
)

// ErrInvalidResult is returned when an expression supposed to evaluate to a document
// returns something else.
var ErrInvalidResult = errors.New("expression must evaluate to a document")

// An Operator is used to modify a stream.
// It takes an environment containing the current value as well as any other metadata
// created by other operatorsand returns a new environment which will be passed to the next operator.
// If it returns a nil environment, the env will be ignored.
// If it returns an error, the stream will be interrupted and that error will bubble up
// and returned by this function, unless that error is ErrStreamClosed, in which case
// the Iterate method will stop the iteration and return nil.
// Stream operators can be reused, and thus, any state or side effect should be kept within the Op closure
// unless the nature of the operator prevents that.
type Operator interface {
	Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error
	SetPrev(prev Operator)
	SetNext(next Operator)
	GetNext() Operator
	GetPrev() Operator
	String() string
}

// An OperatorFunc is the function that will receive each value of the stream.
type OperatorFunc func(func(env *environment.Environment) error) error

func Pipe(ops ...Operator) Operator {
	for i := len(ops) - 1; i > 0; i-- {
		ops[i].SetPrev(ops[i-1])
		ops[i-1].SetNext(ops[i])
	}

	return ops[len(ops)-1]
}

type BaseOperator struct {
	Prev Operator
	Next Operator
}

func (op *BaseOperator) SetPrev(o Operator) {
	op.Prev = o
}

func (op *BaseOperator) SetNext(o Operator) {
	op.Next = o
}

func (op *BaseOperator) GetPrev() Operator {
	return op.Prev
}

func (op *BaseOperator) GetNext() Operator {
	return op.Next
}
