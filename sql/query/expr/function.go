package expr

import (
	"errors"
	"fmt"
	"strings"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/key"
)

var functions = map[string]func(args ...Expr) (Expr, error){
	"pk": func(args ...Expr) (Expr, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("pk() takes no arguments")
		}
		return new(PKFunc), nil
	},
	"count": func(args ...Expr) (Expr, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("COUNT() takes 1 argument")
		}
		return &CountFunc{Expr: args[0]}, nil
	},
}

// GetFunc return a function expression by name.
func GetFunc(name string, args ...Expr) (Expr, error) {
	fn, ok := functions[strings.ToLower(name)]
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
	if ctx.Info == nil {
		return document.Value{}, errors.New("no table specified")
	}

	pk := ctx.Info.GetPrimaryKey()
	if pk != nil {
		return pk.Path.GetValue(ctx.Document)
	}

	return key.DecodeValue(document.IntegerValue, ctx.Document.(document.Keyer).Key())
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (k PKFunc) IsEqual(other Expr) bool {
	_, ok := other.(PKFunc)
	return ok
}

func (k PKFunc) String() string {
	return "pk()"
}

// CastFunc represents the CAST expression.
type CastFunc struct {
	Expr   Expr
	CastAs document.ValueType
}

// Eval returns the primary key of the current document.
func (c CastFunc) Eval(ctx EvalStack) (document.Value, error) {
	v, err := c.Expr.Eval(ctx)
	if err != nil {
		return v, err
	}

	return v.CastAs(c.CastAs)
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (c CastFunc) IsEqual(other Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(CastFunc)
	if !ok {
		return false
	}

	if c.CastAs != o.CastAs {
		return false
	}

	if c.Expr != nil {
		return Equal(c.Expr, o.Expr)
	}

	return o.Expr != nil
}

func (c CastFunc) String() string {
	return fmt.Sprintf("CAST(%v AS %v)", c.Expr, c.CastAs)
}

// CountFunc is the COUNT aggregator function. It aggregates documents
type CountFunc struct {
	Expr  Expr
	Alias string
}

// Eval evaluates Expr and returns 1 if the result is not null.
func (c *CountFunc) Eval(ctx EvalStack) (document.Value, error) {
	return ctx.Document.GetByField(c.String())
}

func (c *CountFunc) SetAlias(alias string) {
	c.Alias = alias
}

func (c *CountFunc) NewAggregator(group document.Value) document.Aggregator {
	return &CountAggregator{
		Fn: c,
	}
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (c *CountFunc) IsEqual(other Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(*CountFunc)
	if !ok {
		return false
	}

	return Equal(c.Expr, o.Expr)
}

func (c *CountFunc) String() string {
	if c.Alias != "" {
		return c.Alias
	}

	return fmt.Sprintf("COUNT(%v)", c.Expr)
}

// CountAggregator is an aggregator that counts non-null expressions.
type CountAggregator struct {
	Fn    *CountFunc
	Count int64
}

// Add increments the counter if the count expression evaluates to a non-null value.
func (c *CountAggregator) Add(d document.Document) error {
	v, err := c.Fn.Expr.Eval(EvalStack{
		Document: d,
	})
	if err != nil {
		return err
	}
	if err != nil && err != document.ErrFieldNotFound {
		return err
	}
	if v != nullLitteral {
		c.Count++
	}

	return nil
}

// Aggregate adds a field to the given buffer with the value of the counter.
func (c *CountAggregator) Aggregate(fb *document.FieldBuffer) error {
	fb.Add(c.Fn.String(), document.NewIntegerValue(c.Count))
	return nil
}
