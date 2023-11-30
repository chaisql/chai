package path

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/object"
	"github.com/genjidb/genji/types"
)

// A SetOperator sets the value of a column or nested field in the current row.
type SetOperator struct {
	stream.BaseOperator
	Path object.Path
	Expr expr.Expr
}

// Set returns a SetOperator that sets the value of a column or nested field in the current row.
func Set(path object.Path, e expr.Expr) *SetOperator {
	return &SetOperator{
		Path: path,
		Expr: e,
	}
}

// Iterate implements the Operator interface.
func (op *SetOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var fb object.FieldBuffer
	var br database.BasicRow
	var newEnv environment.Environment

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		v, err := op.Expr.Eval(out)
		if err != nil && !errors.Is(err, types.ErrFieldNotFound) {
			return err
		}

		r, ok := out.GetRow()
		if !ok {
			return errors.New("missing row")
		}

		fb.Reset()
		err = fb.Copy(r.Object())
		if err != nil {
			return err
		}

		err = fb.Set(op.Path, v)
		if errors.Is(err, types.ErrFieldNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		br.ResetWith(r.TableName(), r.Key(), &fb)

		newEnv.SetOuter(out)
		newEnv.SetRow(&br)

		return f(&newEnv)
	})
}

func (op *SetOperator) String() string {
	return fmt.Sprintf("paths.Set(%s, %s)", op.Path, op.Expr)
}
