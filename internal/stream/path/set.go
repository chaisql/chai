package path

import (
	"fmt"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

// A SetOperator sets the value of a column in the current row.
type SetOperator struct {
	stream.BaseOperator
	Column string
	Expr   expr.Expr
}

// Set returns a SetOperator that sets the value of a column in the current row.
func Set(column string, e expr.Expr) *SetOperator {
	return &SetOperator{
		Column: column,
		Expr:   e,
	}
}

func (op *SetOperator) Clone() stream.Operator {
	return &SetOperator{
		BaseOperator: op.BaseOperator.Clone(),
		Column:       op.Column,
		Expr:         expr.Clone(op.Expr),
	}
}

// Iterate implements the Operator interface.
func (op *SetOperator) Iterator(in *environment.Environment) (stream.Iterator, error) {
	prev, err := op.Prev.Iterator(in)
	if err != nil {
		return nil, err
	}

	return &SetIterator{
		Iterator: prev,
		column:   op.Column,
		expr:     op.Expr,
		env:      in,
	}, nil
}

func (op *SetOperator) String() string {
	return fmt.Sprintf("paths.Set(%s, %s)", op.Column, op.Expr)
}

type SetIterator struct {
	stream.Iterator

	column string
	expr   expr.Expr
	env    *environment.Environment
	buf    row.ColumnBuffer
	dr     database.BasicRow
}

func (it *SetIterator) Row() (database.Row, error) {
	r, err := it.Iterator.Row()
	if err != nil {
		return nil, err
	}

	v, err := it.expr.Eval(it.env.CloneWithRow(r))
	if err != nil && !errors.Is(err, types.ErrColumnNotFound) {
		return nil, err
	}

	it.buf.Reset()
	err = it.buf.Copy(r)
	if err != nil {
		return nil, err
	}

	err = it.buf.Set(it.column, v)
	if err != nil {
		return nil, err
	}

	it.dr.ResetWith(r.TableName(), r.Key(), &it.buf)
	return &it.dr, nil
}
