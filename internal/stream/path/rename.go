package path

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/object"
	"github.com/genjidb/genji/types"
)

// An RenameOperator iterates over all columns of the incoming row in order and renames them.
type RenameOperator struct {
	stream.BaseOperator
	ColumnNames []string
}

// PathsRename iterates over all columns of the incoming row in order and renames them.
// If the number of columns of the incoming row doesn't match the number of expected fields,
// it returns an error.
func PathsRename(columnNames ...string) *RenameOperator {
	return &RenameOperator{
		ColumnNames: columnNames,
	}
}

// Iterate implements the Operator interface.
func (op *RenameOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var fb object.FieldBuffer
	var newEnv environment.Environment

	var br database.BasicRow
	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		fb.Reset()

		r, ok := out.GetRow()
		if !ok {
			return errors.New("missing row")
		}

		var i int
		err := r.Iterate(func(field string, value types.Value) error {
			// if there are too many columns in the incoming row
			if i >= len(op.ColumnNames) {
				n, err := object.Length(r.Object())
				if err != nil {
					return err
				}
				return fmt.Errorf("%d values for %d columns", n, len(op.ColumnNames))
			}

			fb.Add(op.ColumnNames[i], value)
			i++
			return nil
		})
		if err != nil {
			return err
		}

		// if there are too few columns in the incoming row
		if i < len(op.ColumnNames) {
			n, err := object.Length(r.Object())
			if err != nil {
				return err
			}
			return fmt.Errorf("%d values for %d columns", n, len(op.ColumnNames))
		}

		br.ResetWith(r.TableName(), r.Key(), &fb)
		newEnv.SetOuter(out)
		newEnv.SetRow(&br)

		return f(&newEnv)
	})
}

func (op *RenameOperator) String() string {
	return fmt.Sprintf("paths.Rename(%s)", strings.Join(op.ColumnNames, ", "))
}
