package path

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/types"
)

// An RenameOperator iterates over all fields of the incoming document in order and renames them.
type RenameOperator struct {
	stream.BaseOperator
	FieldNames []string
}

// PathsRename iterates over all fields of the incoming document in order and renames them.
// If the number of fields of the incoming document doesn't match the number of expected fields,
// it returns an error.
func PathsRename(fieldNames ...string) *RenameOperator {
	return &RenameOperator{
		FieldNames: fieldNames,
	}
}

// Iterate implements the Operator interface.
func (op *RenameOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var fb document.FieldBuffer
	var newEnv environment.Environment

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		fb.Reset()

		d, ok := out.GetDocument()
		if !ok {
			return errors.New("missing document")
		}

		var i int
		err := d.Iterate(func(field string, value types.Value) error {
			// if there are too many fields in the incoming document
			if i >= len(op.FieldNames) {
				n, err := document.Length(d)
				if err != nil {
					return err
				}
				return fmt.Errorf("%d values for %d fields", n, len(op.FieldNames))
			}

			fb.Add(op.FieldNames[i], value)
			i++
			return nil
		})
		if err != nil {
			return err
		}

		// if there are too few fields in the incoming document
		if i < len(op.FieldNames) {
			n, err := document.Length(d)
			if err != nil {
				return err
			}
			return fmt.Errorf("%d values for %d fields", n, len(op.FieldNames))
		}

		newEnv.SetOuter(out)
		newEnv.SetDocument(&fb)

		return f(&newEnv)
	})
}

func (op *RenameOperator) String() string {
	return fmt.Sprintf("paths.Rename(%s)", strings.Join(op.FieldNames, ", "))
}
