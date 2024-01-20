package table

import (
	"fmt"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/stream"
	"github.com/cockroachdb/errors"
)

// A InsertOperator inserts incoming rows to the table.
type InsertOperator struct {
	stream.BaseOperator
	Name string
}

// Insert inserts incoming rows to the table.
func Insert(tableName string) *InsertOperator {
	return &InsertOperator{Name: tableName}
}

// Iterate implements the Operator interface.
func (op *InsertOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var newEnv environment.Environment

	var table *database.Table

	newBloc := stream.NewRowBloc()

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		newEnv.SetOuter(out)

		bloc, ok := out.GetBloc()
		if !ok {
			return errors.New("missing bloc")
		}

		var err error
		if table == nil {
			table, err = out.GetTx().Catalog.GetTable(out.GetTx(), op.Name)
			if err != nil {
				return err
			}
		}

		r := bloc.Next()
		for r != nil {
			_, newRow, err := table.Insert(r.Object())
			if err != nil {
				return err
			}

			newBloc.Add(newRow)

			r = bloc.Next()
		}

		newEnv.SetBloc(newBloc)

		return f(&newEnv)
	})
}

func (op *InsertOperator) String() string {
	return fmt.Sprintf("table.Insert(%q)", op.Name)
}
