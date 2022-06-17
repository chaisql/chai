package functions

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/types"
)

var builtinFunctions = Definitions{
	"typeof": &definition{
		name:  "typeof",
		arity: 1,
		constructorFn: func(args ...expr.Expr) (expr.Function, error) {
			return &TypeOf{Expr: args[0]}, nil
		},
	},
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
func BuiltinDefinitions() Definitions {
	return builtinFunctions
}

type TypeOf struct {
	Expr expr.Expr
}

func (t *TypeOf) Eval(env *environment.Environment) (types.Value, error) {
	v, err := t.Expr.Eval(env)
	if err != nil {
		return nil, err
	}

	return types.NewTextValue(v.Type().String()), nil
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (t *TypeOf) IsEqual(other expr.Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(*TypeOf)
	if !ok {
		return false
	}

	return expr.Equal(t.Expr, o.Expr)
}

func (t *TypeOf) Params() []expr.Expr { return []expr.Expr{t.Expr} }

func (t *TypeOf) String() string {
	return fmt.Sprintf("typeof(%v)", t.Expr)
}

// PK represents the pk() function.
// It returns the primary key of the current document.
type PK struct{}

// Eval returns the primary key of the current document.
func (k *PK) Eval(env *environment.Environment) (types.Value, error) {
	tableName, ok := env.Get(environment.TableKey)
	if !ok {
		return expr.NullLiteral, nil
	}

	dpk, ok := env.GetKey()
	if !ok {
		return expr.NullLiteral, nil
	}

	vs, err := dpk.Decode()
	if err != nil {
		return expr.NullLiteral, err
	}

	info, err := env.GetCatalog().GetTableInfo(types.As[string](tableName))
	if err != nil {
		return nil, err
	}

	pk := info.GetPrimaryKey()
	if pk != nil {
		for i, tp := range pk.Types {
			if !tp.IsAny() {
				vs[i], err = document.CastAs(vs[i], tp)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	vb := document.NewValueBuffer()

	for _, v := range vs {
		vb.Append(v)
	}

	return types.NewArrayValue(vb), nil
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

var _ expr.AggregatorBuilder = (*Count)(nil)

// Count is the COUNT aggregator function. It counts the number of documents
// in a stream.
type Count struct {
	Expr     expr.Expr
	Wildcard bool
	Count    int64
}

func (c *Count) Eval(env *environment.Environment) (types.Value, error) {
	d, ok := env.GetDocument()
	if !ok {
		return nil, errors.New("misuse of aggregation function COUNT()")
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

	return fmt.Sprintf("COUNT(%v)", c.Expr)
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
	if err != nil && !errors.Is(err, types.ErrFieldNotFound) {
		return err
	}
	if v.Type() != types.NullValue {
		c.Count++
	}

	return nil
}

// Eval returns the result of the aggregation as an integer.
func (c *CountAggregator) Eval(env *environment.Environment) (types.Value, error) {
	return types.NewIntegerValue(c.Count), nil
}

func (c *CountAggregator) String() string {
	return c.Fn.String()
}

// Min is the MIN aggregator function.
type Min struct {
	Expr expr.Expr
}

// Eval extracts the min value from the given document and returns it.
func (m *Min) Eval(env *environment.Environment) (types.Value, error) {
	d, ok := env.GetDocument()
	if !ok {
		return nil, errors.New("misuse of aggregation function MIN()")
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
	return fmt.Sprintf("MIN(%v)", m.Expr)
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
	Min types.Value
}

// Aggregate stores the minimum value. Values are compared based on their types,
// then if the type is equal their value is compared. Numbers are considered of the same type.
func (m *MinAggregator) Aggregate(env *environment.Environment) error {
	v, err := m.Fn.Expr.Eval(env)
	if err != nil && !errors.Is(err, types.ErrFieldNotFound) {
		return err
	}
	if v.Type() == types.NullValue {
		return nil
	}

	// clone the value to avoid it being reused during next aggregation
	v, err = document.CloneValue(v)
	if err != nil {
		return err
	}

	if m.Min == nil {
		m.Min = v
		return nil
	}

	if m.Min.Type() == v.Type() || m.Min.Type().IsNumber() && v.Type().IsNumber() {
		ok, err := types.IsGreaterThan(m.Min, v)
		if err != nil {
			return err
		}
		if ok {
			m.Min = v
		}

		return nil
	}

	if m.Min.Type() > v.Type() {
		m.Min = v
	}

	return nil
}

// Eval return the minimum value.
func (m *MinAggregator) Eval(env *environment.Environment) (types.Value, error) {
	if m.Min == nil {
		return types.NewNullValue(), nil
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
func (m *Max) Eval(env *environment.Environment) (types.Value, error) {
	d, ok := env.GetDocument()
	if !ok {
		return nil, errors.New("misuse of aggregation function MAX()")
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
	return fmt.Sprintf("MAX(%v)", m.Expr)
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
	Max types.Value
}

// Aggregate stores the maximum value. Values are compared based on their types,
// then if the type is equal their value is compared. Numbers are considered of the same type.
func (m *MaxAggregator) Aggregate(env *environment.Environment) error {
	v, err := m.Fn.Expr.Eval(env)
	if err != nil && !errors.Is(err, types.ErrFieldNotFound) {
		return err
	}
	if v.Type() == types.NullValue {
		return nil
	}

	// clone the value to avoid it being reused during next aggregation
	v, err = document.CloneValue(v)
	if err != nil {
		return err
	}

	if m.Max == nil {
		m.Max = v
		return nil
	}

	if m.Max.Type() == v.Type() || m.Max.Type().IsNumber() && v.Type().IsNumber() {
		ok, err := types.IsLesserThan(m.Max, v)
		if err != nil {
			return err
		}
		if ok {
			m.Max = v
		}

		return nil
	}

	if m.Max.Type() < v.Type() {
		m.Max = v
	}

	return nil
}

// Eval return the maximum value.
func (m *MaxAggregator) Eval(env *environment.Environment) (types.Value, error) {
	if m.Max == nil {
		return types.NewNullValue(), nil
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
func (s *Sum) Eval(env *environment.Environment) (types.Value, error) {
	d, ok := env.GetDocument()
	if !ok {
		return nil, errors.New("misuse of aggregation function SUM()")
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
	return fmt.Sprintf("SUM(%v)", s.Expr)
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
	if err != nil && !errors.Is(err, types.ErrFieldNotFound) {
		return err
	}
	if v.Type() != types.IntegerValue && v.Type() != types.DoubleValue {
		return nil
	}

	if s.SumF != nil {
		if v.Type() == types.IntegerValue {
			*s.SumF += float64(types.As[int64](v))
		} else {
			*s.SumF += float64(types.As[float64](v))
		}

		return nil
	}

	if v.Type() == types.DoubleValue {
		var sumF float64
		if s.SumI != nil {
			sumF = float64(*s.SumI)
		}
		s.SumF = &sumF
		*s.SumF += float64(types.As[float64](v))

		return nil
	}

	if s.SumI == nil {
		var sumI int64
		s.SumI = &sumI
	}

	*s.SumI += types.As[int64](v)
	return nil
}

// Eval return the aggregated sum.
func (s *SumAggregator) Eval(env *environment.Environment) (types.Value, error) {
	if s.SumF != nil {
		return types.NewDoubleValue(*s.SumF), nil
	}
	if s.SumI != nil {
		return types.NewIntegerValue(*s.SumI), nil
	}

	return types.NewNullValue(), nil
}

func (s *SumAggregator) String() string {
	return s.Fn.String()
}

// Avg is the AVG aggregator function.
type Avg struct {
	Expr expr.Expr
}

// Eval extracts the average value from the given document and returns it.
func (s *Avg) Eval(env *environment.Environment) (types.Value, error) {
	d, ok := env.GetDocument()
	if !ok {
		return nil, errors.New("misuse of aggregation function AVG()")
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
	return fmt.Sprintf("AVG(%v)", s.Expr)
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
	if err != nil && !errors.Is(err, types.ErrFieldNotFound) {
		return err
	}

	switch v.Type() {
	case types.IntegerValue:
		s.Avg += float64(types.As[int64](v))
	case types.DoubleValue:
		s.Avg += types.As[float64](v)
	default:
		return nil
	}
	s.Counter++

	return nil
}

// Eval returns the aggregated average as a double.
func (s *AvgAggregator) Eval(env *environment.Environment) (types.Value, error) {
	if s.Counter == 0 {
		return types.NewDoubleValue(0), nil
	}

	return types.NewDoubleValue(s.Avg / float64(s.Counter)), nil
}

func (s *AvgAggregator) String() string {
	return s.Fn.String()
}
