package rows

import (
	"fmt"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/types"
)

// A SkipOperator skips the n first values of the stream.
type SkipOperator struct {
	stream.BaseOperator
	E expr.Expr
}

// Skip ignores the first n values of the stream.
func Skip(e expr.Expr) *SkipOperator {
	return &SkipOperator{E: e}
}

// Iterate implements the Operator interface.
func (op *SkipOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	v, err := op.E.Eval(in)
	if err != nil {
		return err
	}

	if !v.Type().IsNumber() {
		return fmt.Errorf("offset expression must evaluate to a number, got %q", v.Type())
	}

	v, err = v.CastAs(types.TypeBigint)
	if err != nil {
		return err
	}

	n := types.AsInt64(v)
	var skipped int64

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		if skipped < n {
			skipped++
			return nil
		}

		return f(out)
	})
}

func (op *SkipOperator) String() string {
	return fmt.Sprintf("rows.Skip(%s)", op.E)
}
