package table

import (
	"fmt"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/stream"
	"github.com/cockroachdb/errors"
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

	return &ReplaceIterator{
		Iterator: prev,
		name:     op.Name,
		in:       in,
	}, nil
}

func (op *ReplaceOperator) String() string {
	return fmt.Sprintf("table.Replace(%q)", op.Name)
}

type ReplaceIterator struct {
	stream.Iterator

	name  string
	table *database.Table
	in    *environment.Environment
}

func (it *ReplaceIterator) Row() (row.Row, error) {
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

	_, err = it.table.Replace(dr.Key(), r)
	if err != nil {
		return nil, err
	}

	return r, nil
}
