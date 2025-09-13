package rows

import (
	"fmt"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/types"
)

// A TakeOperator closes the stream after a certain number of values.
type TakeOperator struct {
	stream.BaseOperator
	E expr.Expr
}

// Take closes the stream after n values have passed through the operator.
func Take(e expr.Expr) *TakeOperator {
	return &TakeOperator{E: e}
}

// Iterate implements the Operator interface.
func (op *TakeOperator) Iterator(in *environment.Environment) (stream.Iterator, error) {
	v, err := op.E.Eval(in)
	if err != nil {
		return nil, err
	}

	if !v.Type().IsNumber() {
		return nil, fmt.Errorf("limit expression must evaluate to a number, got %q", v.Type())
	}

	v, err = v.CastAs(types.TypeBigint)
	if err != nil {
		return nil, err
	}

	prev, err := op.Prev.Iterator(in)
	if err != nil {
		return nil, err
	}

	return &TakeIterator{
		Iterator: prev,
		n:        types.AsInt64(v),
	}, nil
}

func (op *TakeOperator) String() string {
	return fmt.Sprintf("rows.Take(%s)", op.E)
}

type TakeIterator struct {
	stream.Iterator

	count int64
	n     int64
}

func (it *TakeIterator) Next() bool {
	if it.count < it.n {
		it.count++
		return it.Iterator.Next()
	}

	return false
}
