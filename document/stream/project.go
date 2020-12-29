package stream

import (
	"fmt"
	"strings"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
)

// A ProjectOperator applies an expression on each value of the stream and returns a new value.
type ProjectOperator struct {
	Exprs []expr.Expr
}

// Project creates a ProjectOperator.
func Project(exprs ...expr.Expr) *ProjectOperator {
	return &ProjectOperator{Exprs: exprs}
}

// Op implements the Operator interface.
func (m *ProjectOperator) Op() (OperatorFunc, error) {
	var mask maskDocument
	v := document.Value{
		Type: document.DocumentValue,
		V:    &mask,
	}
	var newEnv expr.Environment

	return func(env *expr.Environment) (*expr.Environment, error) {
		mask.Env = env
		mask.Exprs = m.Exprs
		newEnv.SetCurrentValue(v)
		newEnv.Outer = env
		return &newEnv, nil
	}, nil
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

type maskDocument struct {
	Env   *expr.Environment
	Exprs []expr.Expr
}

func (d maskDocument) GetByField(field string) (v document.Value, err error) {
	for _, e := range d.Exprs {
		if _, ok := e.(expr.Wildcard); ok {
			cv, ok := d.Env.GetCurrentValue()
			if !ok {
				continue
			}

			if cv.Type != document.DocumentValue {
				if cv.String() == field {
					return cv, nil
				}

				continue
			}

			v, err = cv.V.(document.Document).GetByField(field)
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

func (d maskDocument) Iterate(fn func(field string, value document.Value) error) error {
	for _, e := range d.Exprs {
		if _, ok := e.(expr.Wildcard); ok {
			v, ok := d.Env.GetCurrentValue()
			if !ok {
				return nil
			}

			if v.Type == document.DocumentValue {
				err := v.V.(document.Document).Iterate(fn)
				if err != nil {
					return err
				}

				continue
			}

			err := fn(v.String(), v)
			if err != nil {
				return err
			}

			continue
		}

		v, err := e.Eval(d.Env)
		if err != nil {
			return err
		}

		err = fn(e.(fmt.Stringer).String(), v)
		if err != nil {
			return err
		}
	}

	return nil
}
