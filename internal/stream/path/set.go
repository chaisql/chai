package path

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/types"
)

// A SetOperator filters duplicate documents.
type SetOperator struct {
	stream.BaseOperator
	Path document.Path
	Expr expr.Expr
}

// Set filters duplicate documents based on one or more expressions.
func Set(path document.Path, e expr.Expr) *SetOperator {
	return &SetOperator{
		Path: path,
		Expr: e,
	}
}

// Iterate implements the Operator interface.
func (op *SetOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var fb document.FieldBuffer
	var newEnv environment.Environment

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		d, ok := out.GetDocument()
		if !ok {
			return errors.New("missing document")
		}

		v, err := op.Expr.Eval(out)
		if err != nil && !errors.Is(err, types.ErrFieldNotFound) {
			return err
		}

		fb.Reset()
		err = fb.Copy(d)
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

		newEnv.SetOuter(out)
		newEnv.SetDocument(&fb)

		return f(&newEnv)
	})
}

func (op *SetOperator) String() string {
	return fmt.Sprintf("paths.Set(%s, %s)", op.Path, op.Expr)
}
