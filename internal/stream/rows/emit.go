package rows

import (
	"fmt"
	"strings"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

type EmitOperator struct {
	stream.BaseOperator
	Exprs []expr.Expr
}

// Emit creates an operator that iterates over the given expressions.
// Each expression must evaluate to an object.
func Emit(exprs ...expr.Expr) *EmitOperator {
	return &EmitOperator{Exprs: exprs}
}

func (op *EmitOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	var newEnv environment.Environment
	newEnv.SetOuter(in)

	for _, e := range op.Exprs {
		v, err := e.Eval(in)
		if err != nil {
			return err
		}
		if v.Type() != types.TypeObject {
			return errors.WithStack(stream.ErrInvalidResult)
		}

		newEnv.SetRowFromObject(types.As[types.Object](v))
		err = fn(&newEnv)
		if err != nil {
			return err
		}
	}

	return nil
}

func (op *EmitOperator) String() string {
	var sb strings.Builder

	sb.WriteString("rows.Emit(")
	for i, e := range op.Exprs {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(e.(fmt.Stringer).String())
	}
	sb.WriteByte(')')

	return sb.String()
}
