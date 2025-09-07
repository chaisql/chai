package table

import (
	"fmt"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/stream"
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

	table, err := in.GetTx().Catalog.GetTable(in.GetTx(), op.Name)
	if err != nil {
		return nil, err
	}

	return &DeleteIterator{
		Iterator: prev,
		name:     op.Name,
		table:    table,
	}, nil
}

func (op *DeleteOperator) String() string {
	return fmt.Sprintf("table.Delete('%s')", op.Name)
}

type DeleteIterator struct {
	stream.Iterator

	name  string
	table *database.Table
	row   database.Row
	err   error
}

func (it *DeleteIterator) Next() bool {
	if !it.Iterator.Next() {
		return false
	}

	r, err := it.Iterator.Row()
	if err != nil {
		it.err = err
		return false
	}

	err = it.table.Delete(r.Key())
	if err != nil {
		it.err = err
		return false
	}

	it.row = r

	return true
}

func (it *DeleteIterator) Row() (database.Row, error) {
	return it.row, it.Error()
}

func (it *DeleteIterator) Error() error {
	if it.err != nil {
		return it.err
	}

	return it.Iterator.Error()
}
