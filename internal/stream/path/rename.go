package path

import (
	"fmt"
	"strings"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/types"
)

// An RenameOperator iterates over all columns of the incoming row in order and renames them.
type RenameOperator struct {
	stream.BaseOperator
	ColumnNames []string
}

// PathsRename iterates over all columns of the incoming row in order and renames them.
// If the number of columns of the incoming row doesn't match the number of expected columns,
// it returns an error.
func PathsRename(columnNames ...string) *RenameOperator {
	return &RenameOperator{
		ColumnNames: columnNames,
	}
}

func (op *RenameOperator) Clone() stream.Operator {
	return &RenameOperator{
		BaseOperator: op.BaseOperator.Clone(),
		// No need to clone the column names, they are immutable.
		ColumnNames: op.ColumnNames,
	}
}

// Iterate implements the Operator interface.
func (op *RenameOperator) Iterator(in *environment.Environment) (stream.Iterator, error) {
	prev, err := op.Prev.Iterator(in)
	if err != nil {
		return nil, err
	}

	return &RenameIterator{
		Iterator:    prev,
		columnNames: op.ColumnNames,
	}, nil
}

func (op *RenameOperator) Columns(env *environment.Environment) ([]string, error) {
	return op.ColumnNames, nil
}

func (op *RenameOperator) String() string {
	return fmt.Sprintf("paths.Rename(%s)", strings.Join(op.ColumnNames, ", "))
}

type RenameIterator struct {
	stream.Iterator

	columnNames []string
	buf         row.ColumnBuffer
	br          database.BasicRow
}

func (it *RenameIterator) Row() (database.Row, error) {
	r, err := it.Iterator.Row()
	if err != nil {
		return nil, err
	}

	n, err := row.Length(r)
	if err != nil {
		return nil, err
	}
	if n != len(it.columnNames) {
		return nil, fmt.Errorf("%d values for %d columns", n, len(it.columnNames))
	}

	var i int
	it.buf.Reset()
	err = r.Iterate(func(column string, value types.Value) error {
		it.buf.Add(it.columnNames[i], value)
		i++
		return nil
	})
	if err != nil {
		return nil, err
	}

	it.br.ResetWith(r.TableName(), r.Key(), &it.buf)
	return &it.br, nil
}
