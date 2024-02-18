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

// Iterate implements the Operator interface.
func (op *ProjectOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	cb := row.NewColumnBuffer()
	var br database.BasicRow

	var newEnv environment.Environment

	if op.Prev == nil {
		for _, e := range op.Exprs {
			if _, ok := e.(expr.Wildcard); ok {
				return errors.New("no table specified")
			}

			v, err := e.Eval(in)
			if err != nil {
				return err
			}

			cb.Add(e.String(), v)
		}

		br.ResetWith("", nil, cb)
		newEnv.SetRow(&br)
		newEnv.SetOuter(in)
		return f(&newEnv)
	}

	return op.Prev.Iterate(in, func(env *environment.Environment) error {
		cb.Reset()

		for _, e := range op.Exprs {
			if _, ok := e.(expr.Wildcard); ok {
				r, ok := env.GetRow()
				if !ok {
					return errors.New("no table specified")
				}

				err := r.Iterate(func(field string, value types.Value) error {
					cb.Add(field, value)
					return nil
				})
				if err != nil {
					return err
				}

				continue
			}

			v, err := e.Eval(env)
			if err != nil {
				return err
			}

			cb.Add(e.String(), v)
		}

		dr, ok := env.GetDatabaseRow()
		if ok {
			br.ResetWith(dr.TableName(), dr.Key(), cb)
		} else {
			br.ResetWith("", nil, cb)
		}
		newEnv.SetRow(&br)

		newEnv.SetOuter(env)
		return f(&newEnv)
	})
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
