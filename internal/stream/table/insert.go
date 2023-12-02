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
	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		newEnv.SetOuter(out)

		r, ok := out.GetRow()
		if !ok {
			return errors.New("missing row")
		}

		var err error
		if table == nil {
			table, err = out.GetTx().Catalog.GetTable(out.GetTx(), op.Name)
			if err != nil {
				return err
			}
		}

		_, r, err = table.Insert(r.Object())
		if err != nil {
			return err
		}

		newEnv.SetRow(r)

		return f(&newEnv)
	})
}

func (op *InsertOperator) String() string {
	return fmt.Sprintf("table.Insert(%q)", op.Name)
}
