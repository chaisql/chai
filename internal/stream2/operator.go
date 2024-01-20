package stream2

import (
	"fmt"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/object"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/types"
	"github.com/pkg/errors"
)

type Operator interface {
	Next(*environment.Environment) (Bloc, error)
	Close() error
}

// A TakeOperator closes the stream after a certain number of values.
type TakeOperator struct {
	E     expr.Expr
	child Operator
}

// Take closes the stream after n values have passed through the operator.
func Take(child Operator, e expr.Expr) *TakeOperator {
	return &TakeOperator{E: e}
}

// Iterate implements the Operator interface.
func (op *TakeOperator) Next(in *environment.Environment) (Bloc, error) {
	v, err := op.E.Eval(in)
	if err != nil {
		return nil, err
	}

	if !v.Type().IsNumber() {
		return nil, fmt.Errorf("limit expression must evaluate to a number, got %q", v.Type())
	}

	v, err = object.CastAsInteger(v)
	if err != nil {
		return nil, err
	}

	n := types.As[int64](v)
	var count int64

	for count < n {
		bloc, err := op.child.Next(in)
		if err != nil {
			return nil, err
		}

		count += int64(bloc.Len())
	}

	bloc, err := op.child.Next(in)

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
