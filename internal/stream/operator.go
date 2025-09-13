package stream

import (
	"strings"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/cockroachdb/errors"
)

// ErrInvalidResult is returned when an expression supposed to evaluate to an object
// returns something else.
var ErrInvalidResult = errors.New("expression must evaluate to an object")

// An Operator is used to modify a stream.
// It takes an environment containing the current value as well as any other metadata
// created by other operators and returns a new environment which will be passed to the next operator.
// If it returns a nil environment, the env will be ignored.
// If it returns an error, the stream will be interrupted and that error will bubble up
// and returned by this function, unless that error is ErrStreamClosed, in which case
// the Iterate method will stop the iteration and return nil.
// Stream operators can be reused, and thus, any state or side effect should be kept within the Op closure
// unless the nature of the operator prevents that.
type Operator interface {
	// Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error
	Iterator(in *environment.Environment) (Iterator, error)
	SetPrev(prev Operator)
	SetNext(next Operator)
	GetNext() Operator
	GetPrev() Operator
	String() string
	Columns(env *environment.Environment) ([]string, error)
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

func (op *BaseOperator) Columns(env *environment.Environment) ([]string, error) {
	if op.Prev == nil {
		return nil, nil
	}

	return op.Prev.Columns(env)
}

type Iterator interface {
	Close() error
	Next() bool
	Error() error
	Row() (database.Row, error)
}

type RowsOperator struct {
	BaseOperator
	Rows    []database.Row
	columns []string
}

// Rows creates an operator that iterates over the given rows.
func Rows(columns []string, rows ...database.Row) *RowsOperator {
	return &RowsOperator{columns: columns, Rows: rows}
}

func (op *RowsOperator) Iterator(in *environment.Environment) (Iterator, error) {
	return &RowsIterator{
		env:    in,
		rows:   op.Rows,
		cursor: -1,
	}, nil
}

func (it *RowsOperator) Columns(env *environment.Environment) ([]string, error) {
	return it.columns, nil
}

func (op *RowsOperator) String() string {
	var sb strings.Builder

	sb.WriteString("rows.Rows(")
	for i := range op.Rows {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("<row>")
	}
	sb.WriteByte(')')

	return sb.String()
}

type RowsIterator struct {
	rows   []database.Row
	cursor int
	env    *environment.Environment
}

func (it *RowsIterator) Next() bool {
	it.cursor++

	return it.cursor < len(it.rows)
}

func (it *RowsIterator) Close() error {
	return nil
}

func (it *RowsIterator) Error() error {
	return nil
}

func (it *RowsIterator) Row() (database.Row, error) {
	return it.rows[it.cursor], nil
}

func (it *RowsIterator) Env() *environment.Environment {
	return it.env
}
