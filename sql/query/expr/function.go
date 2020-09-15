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
	"min": func(args ...Expr) (Expr, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("MIN() takes 1 argument")
		}
		return &MinFunc{Expr: args[0]}, nil
	},
	"max": func(args ...Expr) (Expr, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("MAX() takes 1 argument")
		}
		return &MaxFunc{Expr: args[0]}, nil
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
	Expr     Expr
	Alias    string
	Wildcard bool
}

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

	if c.Wildcard && o.Wildcard {
		return c.Expr == nil && o.Expr == nil
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
	if c.Fn.Wildcard {
		c.Count++
		return nil
	}

	v, err := c.Fn.Expr.Eval(EvalStack{
		Document: d,
	})
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

// MinFunc is the MIN aggregator function.
type MinFunc struct {
	Expr  Expr
	Alias string
}

// Eval extracts the min value from the given document and returns it.
func (m *MinFunc) Eval(ctx EvalStack) (document.Value, error) {
	return ctx.Document.GetByField(m.String())
}

// SetAlias implements the planner.AggregatorBuilder interface.
func (m *MinFunc) SetAlias(alias string) {
	m.Alias = alias
}

// NewAggregator implements the planner.AggregatorBuilder interface.
func (m *MinFunc) NewAggregator(group document.Value) document.Aggregator {
	return &MinAggregator{
		Fn: m,
	}
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (m *MinFunc) IsEqual(other Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(*MinFunc)
	if !ok {
		return false
	}

	return Equal(m.Expr, o.Expr)
}

// String returns the alias if non-zero, otherwise it returns a string representation
// of the count expression.
func (m *MinFunc) String() string {
	if m.Alias != "" {
		return m.Alias
	}

	return fmt.Sprintf("MIN(%v)", m.Expr)
}

// MinAggregator is an aggregator that returns the minimum non-null value.
type MinAggregator struct {
	Fn  *MinFunc
	Min document.Value
}

// Add stores the minimum value. Values are compared based on their types,
// then if the type is equal their value is compared. Numbers are considered of the same type.
func (m *MinAggregator) Add(d document.Document) error {
	v, err := m.Fn.Expr.Eval(EvalStack{
		Document: d,
	})
	if err != nil && err != document.ErrFieldNotFound {
		return err
	}
	if v == nullLitteral {
		return nil
	}

	if m.Min.Type == 0 {
		m.Min = v
		return nil
	}

	if m.Min.Type == v.Type || m.Min.Type.IsNumber() && m.Min.Type.IsNumber() {
		ok, err := m.Min.IsGreaterThan(v)
		if err != nil {
			return err
		}
		if ok {
			m.Min = v
		}

		return nil
	}

	if m.Min.Type > v.Type {
		m.Min = v
	}

	return nil
}

// Aggregate adds a field to the given buffer with the minimum value.
func (m *MinAggregator) Aggregate(fb *document.FieldBuffer) error {
	fb.Add(m.Fn.String(), m.Min)
	return nil
}

// MaxFunc is the MAX aggregator function.
type MaxFunc struct {
	Expr  Expr
	Alias string
}

// Eval extracts the max value from the given document and returns it.
func (m *MaxFunc) Eval(ctx EvalStack) (document.Value, error) {
	return ctx.Document.GetByField(m.String())
}

// SetAlias implements the planner.AggregatorBuilder interface.
func (m *MaxFunc) SetAlias(alias string) {
	m.Alias = alias
}

// NewAggregator implements the planner.AggregatorBuilder interface.
func (m *MaxFunc) NewAggregator(group document.Value) document.Aggregator {
	return &MaxAggregator{
		Fn: m,
	}
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (m *MaxFunc) IsEqual(other Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(*MaxFunc)
	if !ok {
		return false
	}

	return Equal(m.Expr, o.Expr)
}

// String returns the alias if non-zero, otherwise it returns a string representation
// of the count expression.
func (m *MaxFunc) String() string {
	if m.Alias != "" {
		return m.Alias
	}

	return fmt.Sprintf("MAX(%v)", m.Expr)
}

// MaxAggregator is an aggregator that returns the minimum non-null value.
type MaxAggregator struct {
	Fn  *MaxFunc
	Max document.Value
}

// Add stores the maximum value. Values are compared based on their types,
// then if the type is equal their value is compared. Numbers are considered of the same type.
func (m *MaxAggregator) Add(d document.Document) error {
	v, err := m.Fn.Expr.Eval(EvalStack{
		Document: d,
	})
	if err != nil && err != document.ErrFieldNotFound {
		return err
	}
	if v == nullLitteral {
		return nil
	}

	if m.Max.Type == 0 {
		m.Max = v
		return nil
	}

	if m.Max.Type == v.Type || m.Max.Type.IsNumber() && m.Max.Type.IsNumber() {
		ok, err := m.Max.IsLesserThan(v)
		if err != nil {
			return err
		}
		if ok {
			m.Max = v
		}

		return nil
	}

	if m.Max.Type < v.Type {
		m.Max = v
	}

	return nil
}

// Aggregate adds a field to the given buffer with the maximum value.
func (m *MaxAggregator) Aggregate(fb *document.FieldBuffer) error {
	fb.Add(m.Fn.String(), m.Max)
	return nil
}
