package table

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/types"
)

// A InsertOperator inserts incoming documents to the table.
type InsertOperator struct {
	stream.BaseOperator
	Name string
}

// Insert inserts incoming documents to the table.
func Insert(tableName string) *InsertOperator {
	return &InsertOperator{Name: tableName}
}

// Iterate implements the Operator interface.
func (op *InsertOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var newEnv environment.Environment
	newEnv.Set(environment.TableKey, types.NewTextValue(op.Name))

	var table *database.Table
	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		newEnv.SetOuter(out)

		d, ok := out.GetDocument()
		if !ok {
			return errors.New("missing document")
		}

		var err error
		if table == nil {
			table, err = out.GetCatalog().GetTable(out.GetTx(), op.Name)
			if err != nil {
				return err
			}
		}

		key, d, err := table.Insert(d)
		if err != nil {
			return err
		}

		newEnv.SetKey(key)
		newEnv.SetDocument(d)

		return f(&newEnv)
	})
}

func (op *InsertOperator) String() string {
	return fmt.Sprintf("table.Insert(%q)", op.Name)
}
