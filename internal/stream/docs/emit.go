package docs

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/types"
)

type EmitOperator struct {
	stream.BaseOperator
	Exprs []expr.Expr
}

// Emit creates an operator that iterates over the given expressions.
// Each expression must evaluate to a document.
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
		if v.Type() != types.DocumentValue {
			return errors.WithStack(stream.ErrInvalidResult)
		}

		newEnv.SetDocument(types.As[types.Document](v))
		err = fn(&newEnv)
		if err != nil {
			return err
		}
	}

	return nil
}

func (op *EmitOperator) String() string {
	var sb strings.Builder

	sb.WriteString("docs.Emit(")
	for i, e := range op.Exprs {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(e.(fmt.Stringer).String())
	}
	sb.WriteByte(')')

	return sb.String()
}
