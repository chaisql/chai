package rows

import (
	"fmt"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
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

func (op *TakeOperator) Clone() stream.Operator {
	return &TakeOperator{
		BaseOperator: op.BaseOperator.Clone(),
		E:            expr.Clone(op.E),
	}
}

// Iterate implements the Operator interface.
func (op *TakeOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	v, err := op.E.Eval(in)
	if err != nil {
		return err
	}

	if !v.Type().IsNumber() {
		return fmt.Errorf("limit expression must evaluate to a number, got %q", v.Type())
	}

	v, err = v.CastAs(types.TypeBigint)
	if err != nil {
		return err
	}

	n := types.AsInt64(v)
	var count int64
	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		if count < n {
			count++
			return f(out)
		}

		return errors.WithStack(stream.ErrStreamClosed)
	})
}

func (op *TakeOperator) String() string {
	return fmt.Sprintf("rows.Take(%s)", op.E)
}
