package table

import (
	"fmt"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/stream"
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
func (op *ReplaceOperator) Iterator(in *environment.Environment) (stream.Iterator, error) {
	prev, err := op.Prev.Iterator(in)
	if err != nil {
		return nil, err
	}

	table, err := in.GetTx().Catalog.GetTable(in.GetTx(), op.Name)
	if err != nil {
		return nil, err
	}

	return &ReplaceIterator{
		Iterator: prev,
		name:     op.Name,
		table:    table,
	}, nil
}

func (op *ReplaceOperator) String() string {
	return fmt.Sprintf("table.Replace(%q)", op.Name)
}

type ReplaceIterator struct {
	stream.Iterator

	name  string
	table *database.Table
	row   database.Row
	err   error
}

func (it *ReplaceIterator) Next() bool {
	if !it.Iterator.Next() {
		return false
	}

	r, err := it.Iterator.Row()
	if err != nil {
		it.err = err
		return false
	}

	it.row, it.err = it.table.Replace(r.Key(), r)
	return it.err == nil
}

func (it *ReplaceIterator) Row() (database.Row, error) {
	return it.row, it.Error()
}

func (it *ReplaceIterator) Error() error {
	if it.err != nil {
		return it.err
	}

	return it.Iterator.Error()
}
