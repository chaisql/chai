package docs

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/types"
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
	var mask MaskDocument
	var newEnv environment.Environment

	if op.Prev == nil {
		mask.Env = in
		mask.Exprs = op.Exprs
		newEnv.SetDocument(&mask)
		newEnv.SetOuter(in)
		return f(&newEnv)
	}

	return op.Prev.Iterate(in, func(env *environment.Environment) error {
		mask.Env = env
		mask.Exprs = op.Exprs
		newEnv.SetDocument(&mask)
		newEnv.SetOuter(env)
		return f(&newEnv)
	})
}

func (op *ProjectOperator) String() string {
	var b strings.Builder

	b.WriteString("docs.Project(")
	for i, e := range op.Exprs {
		b.WriteString(e.(fmt.Stringer).String())
		if i+1 < len(op.Exprs) {
			b.WriteString(", ")
		}
	}
	b.WriteString(")")
	return b.String()
}

type MaskDocument struct {
	Env   *environment.Environment
	Exprs []expr.Expr
}

func (d *MaskDocument) GetByField(field string) (v types.Value, err error) {
	for _, e := range d.Exprs {
		if _, ok := e.(expr.Wildcard); ok {
			d, ok := d.Env.GetDocument()
			if !ok {
				continue
			}

			v, err = d.GetByField(field)
			if errors.Is(err, types.ErrFieldNotFound) {
				continue
			}
			return
		}

		if ne, ok := e.(*expr.NamedExpr); ok && ne.Name() == field {
			return e.Eval(d.Env)
		}

		if e.(fmt.Stringer).String() == field {
			return e.Eval(d.Env)
		}
	}

	err = types.ErrFieldNotFound
	return
}

func (d *MaskDocument) Iterate(fn func(field string, value types.Value) error) error {
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
			field = e.(fmt.Stringer).String()
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
	b, _ := types.NewDocumentValue(d).MarshalText()
	return string(b)
}

func (d *MaskDocument) MarshalJSON() ([]byte, error) {
	return document.MarshalJSON(d)
}
