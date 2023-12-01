package rows

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/object"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/internal/types"
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
		row, ok := env.GetRow()
		if ok {
			mask.tableName = row.TableName()
			mask.key = row.Key()
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

func (m *RowMask) Object() types.Object {
	return m
}

func (m *RowMask) TableName() string {
	return m.tableName
}

func (m *RowMask) Get(column string) (v types.Value, err error) {
	return m.GetByField(column)
}

func (m *RowMask) GetByField(field string) (v types.Value, err error) {
	for _, e := range m.Exprs {
		if _, ok := e.(expr.Wildcard); ok {
			r, ok := m.Env.GetRow()
			if !ok {
				continue
			}

			v, err = r.Get(field)
			if errors.Is(err, types.ErrFieldNotFound) {
				continue
			}
			return
		}

		if ne, ok := e.(*expr.NamedExpr); ok && ne.Name() == field {
			return e.Eval(m.Env)
		}

		if e.(fmt.Stringer).String() == field {
			return e.Eval(m.Env)
		}
	}

	err = types.ErrFieldNotFound
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
				return err
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

func (m *RowMask) String() string {
	b, _ := types.NewObjectValue(m).MarshalText()
	return string(b)
}

func (d *RowMask) MarshalJSON() ([]byte, error) {
	return object.MarshalJSON(d)
}
