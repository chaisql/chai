package table

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/stream"
)

// A ReplaceOperator replaces objects in the table
type ReplaceOperator struct {
	stream.BaseOperator
	Name string
}

// Replace replaces objects in the table.
func Replace(tableName string) *ReplaceOperator {
	return &ReplaceOperator{Name: tableName}
}

// Iterate implements the Operator interface.
func (op *ReplaceOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var table *database.Table

	it := func(out *environment.Environment) error {
		r, ok := out.GetRow()
		if !ok {
			return errors.New("missing row")
		}

		if table == nil {
			var err error
			table, err = out.GetTx().Catalog.GetTable(out.GetTx(), op.Name)
			if err != nil {
				return err
			}
		}

		_, err := table.Replace(r.Key(), r.Object())
		if err != nil {
			return err
		}

		return f(out)
	}

	if op.Prev == nil {
		return it(in)
	}

	return op.Prev.Iterate(in, it)
}

func (op *ReplaceOperator) String() string {
	return fmt.Sprintf("table.Replace(%q)", op.Name)
}
