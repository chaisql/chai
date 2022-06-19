package path

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/types"
)

// A UnsetOperator filters duplicate documents.
type UnsetOperator struct {
	stream.BaseOperator
	Field string
}

// Unset filters duplicate documents based on one or more expressions.
func Unset(field string) *UnsetOperator {
	return &UnsetOperator{
		Field: field,
	}
}

// Iterate implements the Operator interface.
func (op *UnsetOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var fb document.FieldBuffer
	var newEnv environment.Environment

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		fb.Reset()

		d, ok := out.GetDocument()
		if !ok {
			return errors.New("missing document")
		}

		_, err := d.GetByField(op.Field)
		if err != nil {
			if !errors.Is(err, types.ErrFieldNotFound) {
				return err
			}

			return f(out)
		}

		err = fb.Copy(d)
		if err != nil {
			return err
		}

		err = fb.Delete(document.NewPath(op.Field))
		if err != nil {
			return err
		}

		newEnv.SetOuter(out)
		newEnv.SetDocument(&fb)

		return f(&newEnv)
	})
}

func (op *UnsetOperator) String() string {
	return fmt.Sprintf("paths.Unset(%s)", op.Field)
}
