package rows

import (
	"fmt"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/types"
)

// A FilterOperator filters values based on a given expression.
type FilterOperator struct {
	stream.BaseOperator
	Expr expr.Expr
}

// Filter evaluates e for each incoming value and filters any value whose result is not truthy.
func Filter(e expr.Expr) *FilterOperator {
	return &FilterOperator{Expr: e}
}

// Iterate implements the Operator interface.
func (op *FilterOperator) Iterator(in *environment.Environment) (stream.Iterator, error) {
	prev, err := op.Prev.Iterator(in)
	if err != nil {
		return nil, err
	}

	return &FilterIterator{
		Iterator: prev,
		expr:     op.Expr,
	}, nil
}

func (op *FilterOperator) Clone() stream.Operator {
	return &FilterOperator{
		BaseOperator: op.BaseOperator.Clone(),
		Expr:         expr.Clone(op.Expr),
	}
}

func (op *FilterOperator) String() string {
	return fmt.Sprintf("rows.Filter(%s)", op.Expr)
}

type FilterIterator struct {
	stream.Iterator

	err  error
	expr expr.Expr
	r    row.Row
}

func (it *FilterIterator) Error() error {
	if it.err != nil {
		return it.err
	}

	return it.Iterator.Error()
}

func (it *FilterIterator) Next() bool {
	var env environment.Environment
	env.SetOuter(it.Iterator.Env())

	for it.Iterator.Next() {
		var r row.Row
		r, it.err = it.Iterator.Row()
		if it.err != nil {
			return false
		}

		env.SetRow(r)

		var v types.Value
		v, it.err = it.expr.Eval(&env)
		if it.err != nil {
			return false
		}

		var ok bool
		ok, it.err = types.IsTruthy(v)
		if it.err != nil {
			return false
		}
		if ok {
			return true
		}
	}

	if it.Iterator.Error() != nil {
		it.err = it.Iterator.Error()
		return false
	}

	return true
}
