package table

import (
	"fmt"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/stream"
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
	prev, err := op.Prev.Iterator(in)
	if err != nil {
		return nil, err
	}

	table, err := in.GetTx().Catalog.GetTable(in.GetTx(), op.Name)
	if err != nil {
		return nil, err
	}

	return &InsertIterator{
		Iterator: prev,
		table:    table,
	}, nil
}

func (op *InsertOperator) Columns(env *environment.Environment) ([]string, error) {
	info, err := env.GetTx().Catalog.GetTableInfo(op.Name)
	if err != nil {
		return nil, err
	}

	columns := make([]string, len(info.ColumnConstraints.Ordered))
	for i := range info.ColumnConstraints.Ordered {
		columns[i] = info.ColumnConstraints.Ordered[i].Column
	}

	return columns, nil
}

func (op *InsertOperator) String() string {
	return fmt.Sprintf("table.Insert(%q)", op.Name)
}

type InsertIterator struct {
	stream.Iterator

	table *database.Table
	row   database.Row
	err   error
}

func (it *InsertIterator) Next() bool {
	if !it.Iterator.Next() {
		return false
	}

	it.row, it.err = it.Iterator.Row()
	if it.err != nil {
		return false
	}

	if it.row.Key() == nil {
		_, it.row, it.err = it.table.Insert(it.row)
	} else {
		it.row, it.err = it.table.Put(it.row.Key(), it.row)
	}

	return it.err == nil
}

func (it *InsertIterator) Row() (database.Row, error) {
	return it.row, it.Error()
}

func (it *InsertIterator) Error() error {
	if it.err != nil {
		return it.err
	}

	return it.Iterator.Error()
}
