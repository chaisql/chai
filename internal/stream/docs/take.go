package docs

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/types"
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
func (op *TakeOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	v, err := op.E.Eval(in)
	if err != nil {
		return err
	}

	if !v.Type().IsNumber() {
		return fmt.Errorf("limit expression must evaluate to a number, got %q", v.Type())
	}

	v, err = document.CastAsInteger(v)
	if err != nil {
		return err
	}

	n := types.As[int64](v)
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
	return fmt.Sprintf("docs.Take(%s)", op.E)
}
