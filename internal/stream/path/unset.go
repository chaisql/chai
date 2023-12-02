package path

import (
	"fmt"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/object"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

// A UnsetOperator unsets the value of a column in the current row.
type UnsetOperator struct {
	stream.BaseOperator
	Column string
}

// Unset returns a UnsetOperator that unsets the value of a column in the current row.
func Unset(field string) *UnsetOperator {
	return &UnsetOperator{
		Column: field,
	}
}

// Iterate implements the Operator interface.
func (op *UnsetOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var fb object.FieldBuffer
	var br database.BasicRow
	var newEnv environment.Environment

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		fb.Reset()

		r, ok := out.GetRow()
		if !ok {
			return errors.New("missing row")
		}

		_, err := r.Get(op.Column)
		if err != nil {
			if !errors.Is(err, types.ErrFieldNotFound) {
				return err
			}

			return f(out)
		}

		err = fb.Copy(r.Object())
		if err != nil {
			return err
		}

		err = fb.Delete(object.NewPath(op.Column))
		if err != nil {
			return err
		}

		br.ResetWith(r.TableName(), r.Key(), &fb)
		newEnv.SetOuter(out)
		newEnv.SetRow(&br)

		return f(&newEnv)
	})
}

func (op *UnsetOperator) String() string {
	return fmt.Sprintf("paths.Unset(%s)", op.Column)
}
