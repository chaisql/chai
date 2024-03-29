package rows

import (
	"strings"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/stream"
)

type EmitOperator struct {
	stream.BaseOperator
	Rows    []expr.Row
	columns []string
}

// Emit creates an operator that iterates over the given expressions.
// Each expression must evaluate to an row.
func Emit(columns []string, rows ...expr.Row) *EmitOperator {
	return &EmitOperator{columns: columns, Rows: rows}
}

func (op *EmitOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	var newEnv environment.Environment
	newEnv.SetOuter(in)

	for _, e := range op.Rows {
		r, err := e.Eval(in)
		if err != nil {
			return err
		}

		newEnv.SetRow(r)

		err = fn(&newEnv)
		if err != nil {
			return err
		}
	}

	return nil
}

func (it *EmitOperator) Columns(env *environment.Environment) ([]string, error) {
	return it.columns, nil
}

func (op *EmitOperator) Clone() stream.Operator {
	return &EmitOperator{
		BaseOperator: op.BaseOperator.Clone(),
		Rows:         op.Rows,
	}
}

func (op *EmitOperator) String() string {
	var sb strings.Builder

	sb.WriteString("rows.Emit(")
	for i, e := range op.Rows {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(e.String())
	}
	sb.WriteByte(')')

	return sb.String()
}
