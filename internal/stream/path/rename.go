package path

import (
	"fmt"
	"strings"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

// An RenameOperator iterates over all columns of the incoming row in order and renames them.
type RenameOperator struct {
	stream.BaseOperator
	ColumnNames []string
}

// PathsRename iterates over all columns of the incoming row in order and renames them.
// If the number of columns of the incoming row doesn't match the number of expected columns,
// it returns an error.
func PathsRename(columnNames ...string) *RenameOperator {
	return &RenameOperator{
		ColumnNames: columnNames,
	}
}

func (op *RenameOperator) Clone() stream.Operator {
	return &RenameOperator{
		BaseOperator: op.BaseOperator.Clone(),
		// No need to clone the column names, they are immutable.
		ColumnNames: op.ColumnNames,
	}
}

// Iterate implements the Operator interface.
func (op *RenameOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var cb row.ColumnBuffer
	var newEnv environment.Environment

	var br database.BasicRow
	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		cb.Reset()

		r, ok := out.GetRow()
		if !ok {
			return errors.New("missing row")
		}

		var i int
		err := r.Iterate(func(field string, value types.Value) error {
			// if there are too many columns in the incoming row
			if i >= len(op.ColumnNames) {
				n, err := row.Length(r)
				if err != nil {
					return err
				}
				return fmt.Errorf("%d values for %d columns", n, len(op.ColumnNames))
			}

			cb.Add(op.ColumnNames[i], value)
			i++
			return nil
		})
		if err != nil {
			return err
		}

		// if there are too few columns in the incoming row
		if i < len(op.ColumnNames) {
			n, err := row.Length(r)
			if err != nil {
				return err
			}
			return fmt.Errorf("%d values for %d columns", n, len(op.ColumnNames))
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

func (op *RenameOperator) String() string {
	return fmt.Sprintf("paths.Rename(%s)", strings.Join(op.ColumnNames, ", "))
}
