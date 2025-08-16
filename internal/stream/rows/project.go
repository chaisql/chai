package rows

import (
	"fmt"
	"strings"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/types"
)

// A ProjectOperator applies an expression on each value of the stream and returns a new value.
type ProjectOperator struct {
	stream.BaseOperator
	Exprs []expr.Expr
}

// Project creates a ProjectOperator.
func Project(exprs ...expr.Expr) *ProjectOperator {
	return &ProjectOperator{Exprs: exprs}
}

func (op *ProjectOperator) Clone() stream.Operator {
	exprs := make([]expr.Expr, len(op.Exprs))
	for i, e := range op.Exprs {
		exprs[i] = expr.Clone(e)
	}
	return &ProjectOperator{
		BaseOperator: op.BaseOperator.Clone(),
		Exprs:        exprs,
	}
}

func (op *ProjectOperator) Columns(env *environment.Environment) ([]string, error) {
	var cols, prev []string
	var err error

	for _, e := range op.Exprs {
		if _, ok := e.(expr.Wildcard); ok {
			if prev == nil {
				prev, err = op.Prev.Columns(env)
				if err != nil {
					return nil, err
				}
			}

			cols = append(cols, prev...)
		} else {
			cols = append(cols, e.String())
		}
	}

	return cols, nil
}

func (op *ProjectOperator) String() string {
	var b strings.Builder

	b.WriteString("rows.Project(")
	for i, e := range op.Exprs {
		b.WriteString(e.(fmt.Stringer).String())
		if i+1 < len(op.Exprs) {
			b.WriteString(", ")
		}
	}
	b.WriteString(")")
	return b.String()
}

func (op *ProjectOperator) Iterator(in *environment.Environment) (stream.Iterator, error) {
	if op.Prev == nil {
		return &ExprIterator{
			env:   in,
			exprs: op.Exprs,
		}, nil
	}

	prev, err := op.Prev.Iterator(in)
	if err != nil {
		return nil, err
	}

	return &ProjectIterator{
		Iterator: prev,
		env:      in,
		exprs:    op.Exprs,
	}, nil
}

type ExprIterator struct {
	env   *environment.Environment
	exprs []expr.Expr
	row   *database.BasicRow
	buf   *row.ColumnBuffer
	err   error
}

func (it *ExprIterator) Next() bool {
	it.err = nil

	if it.row != nil {
		return false
	}

	it.buf = row.NewColumnBuffer()

	for _, e := range it.exprs {
		v, err := e.Eval(it.env)
		if err != nil {
			it.err = err
			return false
		}

		it.buf.Add(e.String(), v)
	}
	it.row = database.NewBasicRow(it.buf)

	return true
}

func (it *ExprIterator) Close() error {
	return nil
}

func (it *ExprIterator) Error() error {
	return it.err
}

func (it *ExprIterator) Row() (database.Row, error) {
	return it.row, nil
}

type ProjectIterator struct {
	stream.Iterator

	env   *environment.Environment
	exprs []expr.Expr
	row   database.BasicRow
	buf   row.ColumnBuffer
	err   error
}

func (it *ProjectIterator) Next() bool {
	it.buf.Reset()

	if !it.Iterator.Next() {
		return false
	}

	r, err := it.Iterator.Row()
	if err != nil {
		it.err = err
		return false
	}

	env := it.env.CloneWithRow(r)

	for _, e := range it.exprs {
		if _, ok := e.(expr.Wildcard); ok {
			err = r.Iterate(func(field string, value types.Value) error {
				it.buf.Add(field, value)
				return nil
			})
			if err != nil {
				it.err = err
				return false
			}

			continue
		}

		v, err := e.Eval(env)
		if err != nil {
			it.err = err
			return false
		}

		it.buf.Add(e.String(), v)
	}

	it.row.ResetWith(r.TableName(), it.row.Key(), &it.buf)
	it.row.SetOriginalRow(r)

	return true
}

func (it *ProjectIterator) Error() error {
	return it.err
}

func (it *ProjectIterator) Row() (database.Row, error) {
	return &it.row, nil
}
