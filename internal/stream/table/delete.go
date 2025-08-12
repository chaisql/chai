package table

import (
	"fmt"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/row"
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

func (op *DeleteOperator) Clone() stream.Operator {
	return &DeleteOperator{
		BaseOperator: op.BaseOperator.Clone(),
		Name:         op.Name,
	}
}

// Iterate implements the Operator interface.
func (op *DeleteOperator) Iterator(in *environment.Environment) (stream.Iterator, error) {
	prev, err := op.Prev.Iterator(in)
	if err != nil {
		return nil, err
	}

	return &DeleteIterator{
		Iterator: prev,
		name:     op.Name,
		in:       in,
	}, nil
}

func (op *DeleteOperator) String() string {
	return fmt.Sprintf("table.Delete('%s')", op.Name)
}

type DeleteIterator struct {
	stream.Iterator

	name  string
	table *database.Table
	in    *environment.Environment
}

func (it *DeleteIterator) Row() (row.Row, error) {
	if it.table == nil {
		var err error
		it.table, err = it.in.GetTx().Catalog.GetTable(it.in.GetTx(), it.name)
		if err != nil {
			return nil, err
		}
	}

	dr, ok := it.Iterator.Env().GetDatabaseRow()
	if !ok {
		return nil, errors.New("missing row")
	}

	r, err := it.Iterator.Row()
	if err != nil {
		return nil, err
	}

	err = it.table.Delete(dr.Key())
	if err != nil {
		return nil, err
	}

	return r, nil
}
