package docs

import (
	"fmt"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/types"
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

	v, err = document.CastAsInteger(v)
	if err != nil {
		return err
	}

	n := types.As[int64](v)
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
	return fmt.Sprintf("docs.Skip(%s)", op.E)
}
