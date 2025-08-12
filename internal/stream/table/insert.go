package table

import (
	"fmt"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/tree"
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

func (op *InsertOperator) Clone() stream.Operator {
	return &InsertOperator{
		BaseOperator: op.BaseOperator.Clone(),
		Name:         op.Name,
	}
}

// Iterate implements the Operator interface.
func (op *InsertOperator) Iterator(in *environment.Environment) (stream.Iterator, error) {
	table, err := in.GetTx().Catalog.GetTable(in.GetTx(), op.Name)
	if err != nil {
		return nil, err
	}

	prev, err := op.Prev.Iterator(in)
	if err != nil {
		return nil, err
	}

	return &InsertIterator{
		Iterator: prev,
		table:    table,
	}, nil
}

func (op *InsertOperator) String() string {
	return fmt.Sprintf("table.Insert(%q)", op.Name)
}

type InsertIterator struct {
	stream.Iterator

	table *database.Table
	err   error
	k     *tree.Key
	r     row.Row
}

func (it *InsertIterator) Next() bool {
	if !it.Iterator.Next() {
		return false
	}

	var r row.Row
	r, it.err = it.Iterator.Row()
	if it.err != nil {
		return false
	}

	it.k, it.r, it.err = it.table.Insert(r)
	if it.err != nil {
		return false
	}

	return true
}

func (it *InsertIterator) Key() (*tree.Key, error) {
	if it.err != nil {
		return nil, it.err
	}

	return it.k, nil
}

func (it *InsertIterator) Row() (row.Row, error) {
	if it.err != nil {
		return nil, it.err
	}

	return it.r, nil
}
