package table

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/stream"
)

// A DeleteOperator replaces documents in the table
type DeleteOperator struct {
	stream.BaseOperator
	Name string
}

// Delete deletes documents from the table. Incoming documents must implement the document.Keyer interface.
func Delete(tableName string) *DeleteOperator {
	return &DeleteOperator{Name: tableName}
}

// Iterate implements the Operator interface.
func (op *DeleteOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var table *database.Table

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		if table == nil {
			var err error
			table, err = out.GetCatalog().GetTable(out.GetTx(), op.Name)
			if err != nil {
				return err
			}
		}

		key, ok := out.GetKey()
		if !ok {
			return errors.New("missing key")
		}

		err := table.Delete(key)
		if err != nil {
			return err
		}

		return f(out)
	})
}

func (op *DeleteOperator) String() string {
	return fmt.Sprintf("table.Delete('%s')", op.Name)
}
