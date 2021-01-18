package stream

import (
	"fmt"
	"strings"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
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

// Op implements the Operator interface.
func (op *ProjectOperator) Iterate(in *expr.Environment, f func(out *expr.Environment) error) error {
	var mask MaskDocument
	var newEnv expr.Environment

	return op.Prev.Iterate(in, func(env *expr.Environment) error {
		mask.Env = env
		mask.Exprs = op.Exprs
		newEnv.SetDocument(&mask)
		newEnv.Outer = env
		return f(&newEnv)
	})
}

func (m *ProjectOperator) String() string {
	var b strings.Builder

	b.WriteString("project(")
	for i, e := range m.Exprs {
		b.WriteString(e.(fmt.Stringer).String())
		if i+1 < len(m.Exprs) {
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

func (d MaskDocument) GetByField(field string) (v document.Value, err error) {
	for i := len(d.Exprs); i >= 0; i-- {
		e := d.Exprs[i]

		if _, ok := e.(expr.Wildcard); ok {
			d, ok := d.Env.GetDocument()
			if !ok {
				// TODO: SELECT 1, * LIMIT 10; -> "no tables specified";
				continue
			}

			v, err = d.GetByField(field)
			if err == document.ErrFieldNotFound {
				continue
			}
			return
		}

		if e.(fmt.Stringer).String() == field {
			return e.Eval(d.Env)
		}
	}

	err = document.ErrFieldNotFound
	return
}

func (d MaskDocument) Iterate(fn func(field string, value document.Value) error) error {
	fields := make(map[string]struct{})

	for i := len(d.Exprs); i >= 0; i-- {
		e := d.Exprs[i]

		if _, ok := e.(expr.Wildcard); ok {
			d, ok := d.Env.GetDocument()
			if !ok {
				return nil
			}

			err := d.Iterate(func(field string, value document.Value) error {
				if _, ok := fields[field]; ok {
					return nil
				}

				fields[field] = struct{}{}
				return fn(field, value)
			})
			if err != nil {
				return err
			}

			continue
		}

		field := e.(fmt.Stringer).String()
		if _, ok := fields[field]; ok {
			continue
		}
		fields[field] = struct{}{}

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
