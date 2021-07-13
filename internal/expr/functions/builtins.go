package functions

import (
	"errors"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stringutil"
)

var builtinFunctions = DefinitionsTable{
	"pk": &definition{
		name:  "pk",
		arity: 0,
		constructorFn: func(args ...expr.Expr) (expr.Function, error) {
			return &PK{}, nil
		},
	},
	"count": &definition{
		name:  "count",
		arity: 1,
		constructorFn: func(args ...expr.Expr) (expr.Function, error) {
			return &Count{Expr: args[0]}, nil
		},
	},
	"min": &definition{
		name:  "min",
		arity: 1,
		constructorFn: func(args ...expr.Expr) (expr.Function, error) {
			return &Min{Expr: args[0]}, nil
		},
	},
	"max": &definition{
		name:  "max",
		arity: 1,
		constructorFn: func(args ...expr.Expr) (expr.Function, error) {
			return &Max{Expr: args[0]}, nil
		},
	},
	"sum": &definition{
		name:  "sum",
		arity: 1,
		constructorFn: func(args ...expr.Expr) (expr.Function, error) {
			return &Sum{Expr: args[0]}, nil
		},
	},
	"avg": &definition{
		name:  "avg",
		arity: 1,
		constructorFn: func(args ...expr.Expr) (expr.Function, error) {
			return &Avg{Expr: args[0]}, nil
		},
	},
}

// BuiltinDefinitions returns a map of builtin functions.
func BuiltinDefinitions() DefinitionsTable {
	return builtinFunctions
}

// PK represents the pk() function.
// It returns the primary key of the current document.
type PK struct{}

// Eval returns the primary key of the current document.
func (k *PK) Eval(env *environment.Environment) (document.Value, error) {
	d, ok := env.GetDocument()
	if !ok {
		return expr.NullLiteral, nil
	}

	keyer, ok := d.(document.Keyer)
	if !ok {
		return expr.NullLiteral, nil
	}

	v, err := keyer.Key()
	return v, err
}

func (*PK) Params() []expr.Expr { return nil }

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (k *PK) IsEqual(other expr.Expr) bool {
	_, ok := other.(*PK)
	return ok
}

func (k *PK) String() string {
	return "pk()"
}

// Cast represents the CAST expression.
type Cast struct {
	Expr   expr.Expr
	CastAs document.ValueType
}

// Eval returns the primary key of the current document.
func (c Cast) Eval(env *environment.Environment) (document.Value, error) {
	v, err := c.Expr.Eval(env)
	if err != nil {
		return v, err
	}

	return v.CastAs(c.CastAs)
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (c Cast) IsEqual(other expr.Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(Cast)
	if !ok {
		return false
	}

	if c.CastAs != o.CastAs {
		return false
	}

	if c.Expr != nil {
		return expr.Equal(c.Expr, o.Expr)
	}

	return o.Expr != nil
}

func (c Cast) Params() []expr.Expr { return []expr.Expr{c.Expr} }

func (c Cast) String() string {
	return stringutil.Sprintf("CAST(%v AS %v)", c.Expr, c.CastAs)
}

var _ expr.AggregatorBuilder = (*Count)(nil)

// Count is the COUNT aggregator function. It counts the number of documents
// in a stream.
type Count struct {
	Expr     expr.Expr
	Wildcard bool
	Count    int64
}

func (c *Count) Eval(env *environment.Environment) (document.Value, error) {
	d, ok := env.GetDocument()
	if !ok {
		return document.Value{}, errors.New("misuse of aggregation function COUNT()")
	}

	return d.GetByField(c.String())
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (c *Count) IsEqual(other expr.Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(*Count)
	if !ok {
		return false
	}

	if c.Wildcard && o.Wildcard {
		return c.Expr == nil && o.Expr == nil
	}

	return expr.Equal(c.Expr, o.Expr)
}

func (c *Count) Params() []expr.Expr { return []expr.Expr{c.Expr} }

func (c *Count) String() string {
	if c.Wildcard {
		return "COUNT(*)"
	}

	return stringutil.Sprintf("COUNT(%v)", c.Expr)
}

// Aggregator returns a CountAggregator. It implements the AggregatorBuilder interface.
func (c *Count) Aggregator() expr.Aggregator {
	return &CountAggregator{
		Fn: c,
	}
}

// CountAggregator is an aggregator that counts non-null expressions.
type CountAggregator struct {
	Fn    *Count
	Count int64
}

// Aggregate increments the counter if the count expression evaluates to a non-null value.
func (c *CountAggregator) Aggregate(env *environment.Environment) error {
	if c.Fn.Wildcard {
		c.Count++
		return nil
	}

	v, err := c.Fn.Expr.Eval(env)
	if err != nil && err != document.ErrFieldNotFound {
		return err
	}
	if v != expr.NullLiteral {
		c.Count++
	}

	return nil
}

// Eval returns the result of the aggregation as an integer.
func (c *CountAggregator) Eval(env *environment.Environment) (document.Value, error) {
	return document.NewIntegerValue(c.Count), nil
}

func (c *CountAggregator) String() string {
	return c.Fn.String()
}

// Min is the MIN aggregator function.
type Min struct {
	Expr expr.Expr
}

// Eval extracts the min value from the given document and returns it.
func (m *Min) Eval(env *environment.Environment) (document.Value, error) {
	d, ok := env.GetDocument()
	if !ok {
		return document.Value{}, errors.New("misuse of aggregation function MIN()")
	}

	return d.GetByField(m.String())
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (m *Min) IsEqual(other expr.Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(*Min)
	if !ok {
		return false
	}

	return expr.Equal(m.Expr, o.Expr)
}

func (m *Min) Params() []expr.Expr { return []expr.Expr{m.Expr} }

// String returns the alias if non-zero, otherwise it returns a string representation
// of the count expression.
func (m *Min) String() string {
	return stringutil.Sprintf("MIN(%v)", m.Expr)
}

// Aggregator returns a MinAggregator. It implements the AggregatorBuilder interface.
func (m *Min) Aggregator() expr.Aggregator {
	return &MinAggregator{
		Fn: m,
	}
}

// MinAggregator is an aggregator that returns the minimum non-null value.
type MinAggregator struct {
	Fn  *Min
	Min document.Value
}

// Aggregate stores the minimum value. Values are compared based on their types,
// then if the type is equal their value is compared. Numbers are considered of the same type.
func (m *MinAggregator) Aggregate(env *environment.Environment) error {
	v, err := m.Fn.Expr.Eval(env)
	if err != nil && err != document.ErrFieldNotFound {
		return err
	}
	if v == expr.NullLiteral {
		return nil
	}

	if m.Min.Type == 0 {
		m.Min = v
		return nil
	}

	if m.Min.Type == v.Type || m.Min.Type.IsNumber() && v.Type.IsNumber() {
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

// Eval return the minimum value.
func (m *MinAggregator) Eval(env *environment.Environment) (document.Value, error) {
	if m.Min.Type == 0 {
		return document.NewNullValue(), nil
	}
	return m.Min, nil
}

func (m *MinAggregator) String() string {
	return m.Fn.String()
}

// Max is the MAX aggregator function.
type Max struct {
	Expr expr.Expr
}

// Eval extracts the max value from the given document and returns it.
func (m *Max) Eval(env *environment.Environment) (document.Value, error) {
	d, ok := env.GetDocument()
	if !ok {
		return document.Value{}, errors.New("misuse of aggregation function MAX()")
	}

	return d.GetByField(m.String())
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (m *Max) IsEqual(other expr.Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(*Max)
	if !ok {
		return false
	}

	return expr.Equal(m.Expr, o.Expr)
}

func (m *Max) Params() []expr.Expr { return []expr.Expr{m.Expr} }

// String returns the alias if non-zero, otherwise it returns a string representation
// of the count expression.
func (m *Max) String() string {
	return stringutil.Sprintf("MAX(%v)", m.Expr)
}

// Aggregator returns a MaxAggregator. It implements the AggregatorBuilder interface.
func (m *Max) Aggregator() expr.Aggregator {
	return &MaxAggregator{
		Fn: m,
	}
}

// MaxAggregator is an aggregator that returns the minimum non-null value.
type MaxAggregator struct {
	Fn  *Max
	Max document.Value
}

// Aggregate stores the maximum value. Values are compared based on their types,
// then if the type is equal their value is compared. Numbers are considered of the same type.
func (m *MaxAggregator) Aggregate(env *environment.Environment) error {
	v, err := m.Fn.Expr.Eval(env)
	if err != nil && err != document.ErrFieldNotFound {
		return err
	}
	if v == expr.NullLiteral {
		return nil
	}

	if m.Max.Type == 0 {
		m.Max = v
		return nil
	}

	if m.Max.Type == v.Type || m.Max.Type.IsNumber() && v.Type.IsNumber() {
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

// Eval return the maximum value.
func (m *MaxAggregator) Eval(env *environment.Environment) (document.Value, error) {
	if m.Max.Type == 0 {
		return document.NewNullValue(), nil
	}

	return m.Max, nil
}

func (m *MaxAggregator) String() string {
	return m.Fn.String()
}

// Sum is the SUM aggregator function.
type Sum struct {
	Expr expr.Expr
}

// Eval extracts the sum value from the given document and returns it.
func (s *Sum) Eval(env *environment.Environment) (document.Value, error) {
	d, ok := env.GetDocument()
	if !ok {
		return document.Value{}, errors.New("misuse of aggregation function SUM()")
	}

	return d.GetByField(s.String())
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (s *Sum) IsEqual(other expr.Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(*Sum)
	if !ok {
		return false
	}

	return expr.Equal(s.Expr, o.Expr)
}

func (s *Sum) Params() []expr.Expr { return []expr.Expr{s.Expr} }

// String returns the alias if non-zero, otherwise it returns a string representation
// of the count expression.
func (s *Sum) String() string {
	return stringutil.Sprintf("SUM(%v)", s.Expr)
}

// Aggregator returns a Sum. It implements the AggregatorBuilder interface.
func (s *Sum) Aggregator() expr.Aggregator {
	return &SumAggregator{
		Fn: s,
	}
}

// SumAggregator is an aggregator that returns the minimum non-null value.
type SumAggregator struct {
	Fn   *Sum
	SumI *int64
	SumF *float64
}

// Aggregate stores the sum of all non-NULL numeric values in the group.
// The result is an integer value if all summed values are integers.
// If any of the value is a double, the returned result will be a double.
func (s *SumAggregator) Aggregate(env *environment.Environment) error {
	v, err := s.Fn.Expr.Eval(env)
	if err != nil && err != document.ErrFieldNotFound {
		return err
	}
	if v.Type != document.IntegerValue && v.Type != document.DoubleValue {
		return nil
	}

	if s.SumF != nil {
		if v.Type == document.IntegerValue {
			*s.SumF += float64(v.V.(int64))
		} else {
			*s.SumF += float64(v.V.(float64))
		}

		return nil
	}

	if v.Type == document.DoubleValue {
		var sumF float64
		if s.SumI != nil {
			sumF = float64(*s.SumI)
		}
		s.SumF = &sumF
		*s.SumF += float64(v.V.(float64))

		return nil
	}

	if s.SumI == nil {
		var sumI int64
		s.SumI = &sumI
	}

	*s.SumI += v.V.(int64)
	return nil
}

// Eval return the aggregated sum.
func (s *SumAggregator) Eval(env *environment.Environment) (document.Value, error) {
	if s.SumF != nil {
		return document.NewDoubleValue(*s.SumF), nil
	}
	if s.SumI != nil {
		return document.NewIntegerValue(*s.SumI), nil
	}

	return document.NewNullValue(), nil
}

func (s *SumAggregator) String() string {
	return s.Fn.String()
}

// Avg is the AVG aggregator function.
type Avg struct {
	Expr expr.Expr
}

// Eval extracts the average value from the given document and returns it.
func (s *Avg) Eval(env *environment.Environment) (document.Value, error) {
	d, ok := env.GetDocument()
	if !ok {
		return document.Value{}, errors.New("misuse of aggregation function AVG()")
	}

	return d.GetByField(s.String())
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (s *Avg) IsEqual(other expr.Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(*Avg)
	if !ok {
		return false
	}

	return expr.Equal(s.Expr, o.Expr)
}

func (s *Avg) Params() []expr.Expr { return []expr.Expr{s.Expr} }

// String returns the alias if non-zero, otherwise it returns a string representation
// of the average expression.
func (s *Avg) String() string {
	return stringutil.Sprintf("AVG(%v)", s.Expr)
}

// Aggregator returns a Avg. It implements the AggregatorBuilder interface.
func (s *Avg) Aggregator() expr.Aggregator {
	return &AvgAggregator{
		Fn: s,
	}
}

// AvgAggregator is an aggregator that returns the average non-null value.
type AvgAggregator struct {
	Fn      *Avg
	Avg     float64
	Counter int64
}

// Aggregate stores the average value of all non-NULL numeric values in the group.
func (s *AvgAggregator) Aggregate(env *environment.Environment) error {
	v, err := s.Fn.Expr.Eval(env)
	if err != nil && err != document.ErrFieldNotFound {
		return err
	}

	switch v.Type {
	case document.IntegerValue:
		s.Avg += float64(v.V.(int64))
	case document.DoubleValue:
		s.Avg += v.V.(float64)
	default:
		return nil
	}
	s.Counter++

	return nil
}

// Eval returns the aggregated average as a double.
func (s *AvgAggregator) Eval(env *environment.Environment) (document.Value, error) {
	if s.Counter == 0 {
		return document.NewDoubleValue(0), nil
	}

	return document.NewDoubleValue(s.Avg / float64(s.Counter)), nil
}

func (s *AvgAggregator) String() string {
	return s.Fn.String()
}
