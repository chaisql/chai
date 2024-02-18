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

// Iterate implements the Operator interface.
func (op *ProjectOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var mask RowMask
	var newEnv environment.Environment

	if op.Prev == nil {
		mask.Env = in
		mask.Exprs = op.Exprs
		newEnv.SetRow(&mask)
		newEnv.SetOuter(in)
		return f(&newEnv)
	}

	return op.Prev.Iterate(in, func(env *environment.Environment) error {
		r, ok := env.GetDatabaseRow()
		if ok {
			mask.tableName = r.TableName()
			mask.key = r.Key()
		}
		mask.Env = env
		mask.Exprs = op.Exprs
		newEnv.SetRow(&mask)
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

type RowMask struct {
	Env       *environment.Environment
	Exprs     []expr.Expr
	key       *tree.Key
	tableName string
}

func (m *RowMask) Key() *tree.Key {
	return m.key
}

func (m *RowMask) TableName() string {
	return m.tableName
}

func (m *RowMask) Get(column string) (v types.Value, err error) {
	for _, e := range m.Exprs {
		if _, ok := e.(expr.Wildcard); ok {
			r, ok := m.Env.GetRow()
			if !ok {
				continue
			}

			v, err = r.Get(column)
			if errors.Is(err, types.ErrColumnNotFound) {
				continue
			}
			return
		}

		if ne, ok := e.(*expr.NamedExpr); ok && ne.Name() == column {
			return e.Eval(m.Env)
		}

		if col, ok := e.(expr.Column); ok && col.String() == column {
			return e.Eval(m.Env)
		}

		if e.(fmt.Stringer).String() == column {
			return e.Eval(m.Env)
		}
	}

	err = errors.Wrapf(types.ErrColumnNotFound, "%s not found", column)
	return
}

func (m *RowMask) Iterate(fn func(field string, value types.Value) error) error {
	for _, e := range m.Exprs {
		if _, ok := e.(expr.Wildcard); ok {
			r, ok := m.Env.GetRow()
			if !ok {
				return nil
			}

			err := r.Iterate(fn)
			if err != nil {
				return errors.Wrap(err, "wildcard iteration")
			}

			continue
		}

		var col string
		if ne, ok := e.(*expr.NamedExpr); ok {
			col = ne.Name()
		} else {
			col = e.(fmt.Stringer).String()
		}

		v, err := e.Eval(m.Env)
		if err != nil {
			return err
		}

		err = fn(col, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *RowMask) MarshalJSON() ([]byte, error) {
	return row.MarshalJSON(m)
}
