package table

import (
	"fmt"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/stream"
	"github.com/cockroachdb/errors"
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

func (op *ReplaceOperator) Clone() stream.Operator {
	return &ReplaceOperator{
		BaseOperator: op.BaseOperator.Clone(),
		Name:         op.Name,
	}
}

// Iterate implements the Operator interface.
func (op *ReplaceOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var table *database.Table

	it := func(out *environment.Environment) error {
		r, ok := out.GetDatabaseRow()
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

		_, err := table.Replace(r.Key(), r)
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
