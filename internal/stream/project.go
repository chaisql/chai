package stream

import (
	"strings"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stringutil"
)

// A ProjectOperator applies an expression on each value of the stream and returns a new value.
type ProjectOperator struct {
	baseOperator
	Exprs []expr.Expr
}

// Project creates a ProjectOperator.
func Project(exprs ...expr.Expr) *ProjectOperator {
	return &ProjectOperator{Exprs: exprs}
}

// Iterate implements the Operator interface.
func (op *ProjectOperator) Iterate(in *expr.Environment, f func(out *expr.Environment) error) error {
	var mask MaskDocument
	var newEnv expr.Environment

	if op.Prev == nil {
		mask.Env = in
		mask.Exprs = op.Exprs
		newEnv.SetDocument(&mask)
		newEnv.Outer = in
		return f(&newEnv)
	}

	return op.Prev.Iterate(in, func(env *expr.Environment) error {
		mask.Env = env
		mask.Exprs = op.Exprs
		newEnv.SetDocument(&mask)
		newEnv.Outer = env
		return f(&newEnv)
	})
}

func (op *ProjectOperator) String() string {
	var b strings.Builder

	b.WriteString("project(")
	for i, e := range op.Exprs {
		b.WriteString(e.(stringutil.Stringer).String())
		if i+1 < len(op.Exprs) {
			b.WriteString(", ")
		}
	}
	b.WriteString(")")
	return b.String()
}

type MaskDocument struct {
	Env   *expr.Environment
	Exprs []expr.Expr
}

func (d *MaskDocument) GetByField(field string) (v document.Value, err error) {
	for _, e := range d.Exprs {
		if _, ok := e.(expr.Wildcard); ok {
			d, ok := d.Env.GetDocument()
			if !ok {
				continue
			}

			v, err = d.GetByField(field)
			if err == document.ErrFieldNotFound {
				continue
			}
			return
		}

		if ne, ok := e.(*expr.NamedExpr); ok && ne.Name() == field {
			return e.Eval(d.Env)
		}

		if e.(stringutil.Stringer).String() == field {
			return e.Eval(d.Env)
		}
	}

	err = document.ErrFieldNotFound
	return
}

func (d *MaskDocument) Iterate(fn func(field string, value document.Value) error) error {
	for _, e := range d.Exprs {
		if _, ok := e.(expr.Wildcard); ok {
			d, ok := d.Env.GetDocument()
			if !ok {
				return nil
			}

			err := d.Iterate(fn)
			if err != nil {
				return err
			}

			continue
		}

		var field string
		if ne, ok := e.(*expr.NamedExpr); ok {
			field = ne.Name()
		} else {
			field = e.(stringutil.Stringer).String()
		}

		v, err := e.Eval(d.Env)
		if err != nil {
			return err
		}

		err = fn(field, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *MaskDocument) String() string {
	b, _ := document.MarshalJSON(d)
	return string(b)
}

func (d *MaskDocument) MarshalJSON() ([]byte, error) {
	return document.MarshalJSON(d)
}
