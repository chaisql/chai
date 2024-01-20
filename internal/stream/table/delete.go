package table

import (
	"fmt"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/stream"
	"github.com/cockroachdb/errors"
)

// A DeleteOperator replaces objects in the table
type DeleteOperator struct {
	stream.BaseOperator
	Name string
}

// Delete deletes rows from the table.
func Delete(tableName string) *DeleteOperator {
	return &DeleteOperator{Name: tableName}
}

// Iterate implements the Operator interface.
func (op *DeleteOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var table *database.Table

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		if table == nil {
			var err error
			table, err = out.GetTx().Catalog.GetTable(out.GetTx(), op.Name)
			if err != nil {
				return err
			}
		}

		bloc, ok := out.GetBloc()
		if !ok {
			return errors.New("missing bloc")
		}

		r := bloc.Next()
		for r != nil {
			err := table.Delete(r.Key())
			if err != nil {
				return err
			}

			r = bloc.Next()
		}

		return f(out)
	})
}

func (op *DeleteOperator) String() string {
	return fmt.Sprintf("table.Delete('%s')", op.Name)
}
