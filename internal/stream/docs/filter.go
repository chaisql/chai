package docs

import (
	"fmt"

	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/types"
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
func (op *FilterOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		v, err := op.Expr.Eval(out)
		if err != nil {
			return err
		}

		ok, err := types.IsTruthy(v)
		if err != nil || !ok {
			return err
		}

		return f(out)
	})
}

func (op *FilterOperator) String() string {
	return fmt.Sprintf("docs.Filter(%s)", op.Expr)
}
