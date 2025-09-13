package rows

import (
	"strings"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/stream"
)

type EmitOperator struct {
	stream.BaseOperator
	Rows    []expr.Row
	columns []string
}

// Emit creates an operator that iterates over the given expressions.
// Each expression must evaluate to an row.
func Emit(columns []string, rows ...expr.Row) *EmitOperator {
	return &EmitOperator{columns: columns, Rows: rows}
}

func (op *EmitOperator) Iterator(in *environment.Environment) (stream.Iterator, error) {
	return &EmitIterator{
		env:    in,
		rows:   op.Rows,
		cursor: -1,
	}, nil
}

func (it *EmitOperator) Columns(env *environment.Environment) ([]string, error) {
	return it.columns, nil
}

func (op *EmitOperator) String() string {
	var sb strings.Builder

	sb.WriteString("rows.Emit(")
	for i, e := range op.Rows {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(e.String())
	}
	sb.WriteByte(')')

	return sb.String()
}

type EmitIterator struct {
	env    *environment.Environment
	rows   []expr.Row
	cursor int
	row    database.BasicRow
}

func (it *EmitIterator) Next() bool {
	it.cursor++

	return it.cursor < len(it.rows)
}

func (it *EmitIterator) Close() error {
	return nil
}

func (it *EmitIterator) Valid() bool {
	return it.cursor < len(it.rows)
}

func (it *EmitIterator) Error() error {
	return nil
}

func (it *EmitIterator) Row() (database.Row, error) {
	r, err := it.rows[it.cursor].Eval(it.env)
	if err != nil {
		return nil, err
	}

	it.row.ResetWith("", nil, r)
	return &it.row, nil
}

func (it *EmitIterator) Env() *environment.Environment {
	return it.env
}
