package rows

import (
	"fmt"
	"strings"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/tree"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
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
		exprs:    op.Exprs,
	}, nil
}

type ExprIterator struct {
	env   *environment.Environment
	exprs []expr.Expr
	buf   *row.ColumnBuffer
}

func (it *ExprIterator) Next() bool {
	return false
}

func (it *ExprIterator) Close() error {
	return nil
}

func (it *ExprIterator) Valid() bool {
	return it.buf == nil
}

func (it *ExprIterator) Error() error {
	return nil
}

func (it *ExprIterator) Key() (*tree.Key, error) {
	return nil, errors.New("row has no primary key")
}

func (it *ExprIterator) Row() (row.Row, error) {
	if it.buf != nil {
		return it.buf, nil
	}

	it.buf = row.NewColumnBuffer()

	for _, e := range it.exprs {
		v, err := e.Eval(it.env)
		if err != nil {
			return nil, err
		}

		it.buf.Add(e.String(), v)
	}

	return it.buf, nil
}

func (it *ExprIterator) TableName() (string, error) {
	return "", errors.New("row has no table name")
}

func (it *ExprIterator) Env() *environment.Environment {
	return it.env
}

type ProjectIterator struct {
	stream.Iterator

	exprs []expr.Expr
	buf   row.ColumnBuffer
}

func (it *ProjectIterator) Row() (row.Row, error) {
	it.buf.Reset()

	for _, e := range it.exprs {
		if _, ok := e.(expr.Wildcard); ok {
			r, err := it.Iterator.Row()
			if err != nil {
				return nil, err
			}

			err = r.Iterate(func(field string, value types.Value) error {
				it.buf.Add(field, value)
				return nil
			})
			if err != nil {
				return nil, err
			}

			continue
		}

		v, err := e.Eval(it.Iterator.Env())
		if err != nil {
			return nil, err
		}

		it.buf.Add(e.String(), v)
	}

	return &it.buf, nil
}
