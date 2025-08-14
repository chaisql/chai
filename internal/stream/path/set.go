package path

import (
	"fmt"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

// A SetOperator sets the value of a column in the current row.
type SetOperator struct {
	stream.BaseOperator
	Column string
	Expr   expr.Expr
}

// Set returns a SetOperator that sets the value of a column in the current row.
func Set(column string, e expr.Expr) *SetOperator {
	return &SetOperator{
		Column: column,
		Expr:   e,
	}
}

func (op *SetOperator) Clone() stream.Operator {
	return &SetOperator{
		BaseOperator: op.BaseOperator.Clone(),
		Column:       op.Column,
		Expr:         expr.Clone(op.Expr),
	}
}

// Iterate implements the Operator interface.
func (op *SetOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var cb row.ColumnBuffer
	var br database.BasicRow
	var newEnv environment.Environment

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		v, err := op.Expr.Eval(out)
		if err != nil && !errors.Is(err, types.ErrColumnNotFound) {
			return err
		}

		r, ok := out.GetRow()
		if !ok {
			return errors.New("missing row")
		}

		cb.Reset()
		err = cb.Copy(r)
		if err != nil {
			return err
		}

		err = cb.Set(op.Column, v)
		if errors.Is(err, types.ErrColumnNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		newEnv.SetOuter(out)
		if dr, ok := r.(database.Row); ok {
			br.ResetWith(dr.TableName(), dr.Key(), &cb)
			newEnv.SetRow(&br)
		} else {
			newEnv.SetRow(&cb)
		}

		return f(&newEnv)
	})
}

func (op *SetOperator) String() string {
	return fmt.Sprintf("paths.Set(%s, %s)", op.Column, op.Expr)
}
