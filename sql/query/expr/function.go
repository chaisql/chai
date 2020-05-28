package expr

import (
	"errors"
	"fmt"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding"
)

var functions = map[string]func(args ...Expr) (Expr, error){
	"pk": func(args ...Expr) (Expr, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("pk() takes no arguments")
		}
		return new(PKFunc), nil
	},
}

// GetFunc return a function expression by name.
func GetFunc(name string, args ...Expr) (Expr, error) {
	fn, ok := functions[name]
	if !ok {
		return nil, fmt.Errorf("no such function: %q", name)
	}

	return fn(args...)
}

// PKFunc represents the pk() function.
// It returns the primary key of the current document.
type PKFunc struct{}

// Eval returns the primary key of the current document.
func (k PKFunc) Eval(ctx EvalStack) (document.Value, error) {
	if ctx.Cfg == nil {
		return document.Value{}, errors.New("no table specified")
	}

	pk := ctx.Cfg.GetPrimaryKey()
	if pk != nil {
		return pk.Path.GetValue(ctx.Document)
	}

	return encoding.DecodeValue(document.Int64Value, ctx.Document.(document.Keyer).Key())
}

// Cast represents the CAST expression.
type Cast struct {
	Expr      Expr
	ConvertTo document.ValueType
}

// Eval returns the primary key of the current document.
func (c Cast) Eval(ctx EvalStack) (document.Value, error) {
	v, err := c.Expr.Eval(ctx)
	if err != nil {
		return v, err
	}

	return v.ConvertTo(c.ConvertTo)
}
