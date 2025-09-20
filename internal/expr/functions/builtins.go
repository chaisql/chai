package functions

import (
	"fmt"
	"strings"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

var builtinFunctions = Definitions{
	"typeof": &definition{
		name:  "typeof",
		arity: 1,
		constructorFn: func(args ...expr.Expr) (expr.Function, error) {
			return &TypeOf{Expr: args[0]}, nil
		},
	},
	"count": &definition{
		name:  "count",
		arity: 1,
		constructorFn: func(args ...expr.Expr) (expr.Function, error) {
			return NewCount(args[0]), nil
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
	"len": &definition{
		name:  "len",
		arity: 1,
		constructorFn: func(args ...expr.Expr) (expr.Function, error) {
			return &Len{Expr: args[0]}, nil
		},
	},
	"coalesce": &definition{
		name:  "coalesce",
		arity: variadicArity,
		constructorFn: func(args ...expr.Expr) (expr.Function, error) {
			return &Coalesce{Exprs: args}, nil
		},
	},
	"now": &definition{
		name:  "now",
		arity: 0,
		constructorFn: func(args ...expr.Expr) (expr.Function, error) {
			return &Now{}, nil
		},
	},

	"lower": &definition{
		name:  "lower",
		arity: 1,
		constructorFn: func(args ...expr.Expr) (expr.Function, error) {
			return &Lower{Expr: args[0]}, nil
		},
	},
	"upper": &definition{
		name:  "upper",
		arity: 1,
		constructorFn: func(args ...expr.Expr) (expr.Function, error) {
			return &Upper{Expr: args[0]}, nil
		},
	},
	"trim": &definition{
		name:  "trim",
		arity: variadicArity,
		constructorFn: func(args ...expr.Expr) (expr.Function, error) {
			return &Trim{Expr: args, TrimFunc: strings.Trim, Name: "TRIM"}, nil
		},
	},
	"ltrim": &definition{
		name:  "ltrim",
		arity: variadicArity,
		constructorFn: func(args ...expr.Expr) (expr.Function, error) {
			return &Trim{Expr: args, TrimFunc: strings.TrimLeft, Name: "LTRIM"}, nil
		},
	},
	"rtrim": &definition{
		name:  "rtrim",
		arity: variadicArity,
		constructorFn: func(args ...expr.Expr) (expr.Function, error) {
			return &Trim{Expr: args, TrimFunc: strings.TrimRight, Name: "RTRIM"}, nil
		},
	},
	"nextval": &definition{
		name:  "nextval",
		arity: 1,
		constructorFn: func(args ...expr.Expr) (expr.Function, error) {
			return &NextVal{Expr: args[0]}, nil
		},
	},

	"floor":  floor,
	"abs":    abs,
	"acos":   acos,
	"acosh":  acosh,
	"asin":   asin,
	"asinh":  asinh,
	"atan":   atan,
	"atan2":  atan2,
	"random": random,
	"sqrt":   sqrt,
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

var _ expr.AggregatorBuilder = (*Count)(nil)

// Count is the COUNT aggregator function. It counts the number of objects
// in a stream.
type Count struct {
	Expr     expr.Expr
	wildcard bool
	Count    int64
}

func NewCount(e expr.Expr) *Count {
	_, wc := e.(expr.Wildcard)
	return &Count{
		Expr:     e,
		wildcard: wc,
	}
}

func (c *Count) Eval(env *environment.Environment) (types.Value, error) {
	d, ok := env.GetRow()
	if !ok {
		return nil, errors.New("misuse of aggregation function COUNT()")
	}

	return d.Get(c.String())
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

	return expr.Equal(c.Expr, o.Expr)
}

func (c *Count) Params() []expr.Expr { return []expr.Expr{c.Expr} }

func (c *Count) String() string {
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
	if c.Fn.wildcard {
		c.Count++
		return nil
	}

	v, err := c.Fn.Expr.Eval(env)
	if err != nil && !errors.Is(err, types.ErrColumnNotFound) {
		return err
	}
	if v.Type() != types.TypeNull {
		c.Count++
	}

	return nil
}

// Eval returns the result of the aggregation as an integer.
func (c *CountAggregator) Eval(_ *environment.Environment) (types.Value, error) {
	return types.NewBigintValue(c.Count), nil
}

func (c *CountAggregator) String() string {
	return c.Fn.String()
}

// Min is the MIN aggregator function.
type Min struct {
	Expr expr.Expr
}

// Eval extracts the min value from the given object and returns it.
func (m *Min) Eval(env *environment.Environment) (types.Value, error) {
	r, ok := env.GetRow()
	if !ok {
		return nil, errors.New("misuse of aggregation function MIN()")
	}

	return r.Get(m.String())
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
	if err != nil && !errors.Is(err, types.ErrColumnNotFound) {
		return err
	}
	if v.Type() == types.TypeNull {
		return nil
	}

	// // clone the value to avoid it being reused during next aggregation
	// v, err = row.CloneValue(v)
	// if err != nil {
	// 	return err
	// }

	if m.Min == nil {
		m.Min = v
		return nil
	}

	if m.Min.Type() == v.Type() || m.Min.Type().IsNumber() && v.Type().IsNumber() {
		ok, err := m.Min.GT(v)
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
func (m *MinAggregator) Eval(_ *environment.Environment) (types.Value, error) {
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

// Eval extracts the max value from the given object and returns it.
func (m *Max) Eval(env *environment.Environment) (types.Value, error) {
	r, ok := env.GetRow()
	if !ok {
		return nil, errors.New("misuse of aggregation function MAX()")
	}

	return r.Get(m.String())
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
	if err != nil && !errors.Is(err, types.ErrColumnNotFound) {
		return err
	}
	if v.Type() == types.TypeNull {
		return nil
	}

	if m.Max == nil {
		m.Max = v
		return nil
	}

	if m.Max.Type() == v.Type() || m.Max.Type().IsNumber() && v.Type().IsNumber() {
		ok, err := m.Max.LT(v)
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
func (m *MaxAggregator) Eval(_ *environment.Environment) (types.Value, error) {
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

// Eval extracts the sum value from the given object and returns it.
func (s *Sum) Eval(env *environment.Environment) (types.Value, error) {
	r, ok := env.GetRow()
	if !ok {
		return nil, errors.New("misuse of aggregation function SUM()")
	}

	return r.Get(s.String())
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
	if err != nil && !errors.Is(err, types.ErrColumnNotFound) {
		return err
	}
	if !v.Type().IsNumber() {
		return nil
	}

	if s.SumF != nil {
		switch v.Type() {
		case types.TypeInteger, types.TypeBigint:
			*s.SumF += float64(types.AsInt64(v))
		default:
			*s.SumF += float64(types.AsFloat64(v))
		}

		return nil
	}

	if v.Type() == types.TypeDoublePrecision {
		var sumF float64
		if s.SumI != nil {
			sumF = float64(*s.SumI)
		}
		s.SumF = &sumF
		*s.SumF += float64(types.AsFloat64(v))

		return nil
	}

	if s.SumI == nil {
		var sumI int64
		s.SumI = &sumI
	}

	*s.SumI += types.AsInt64(v)
	return nil
}

// Eval return the aggregated sum.
func (s *SumAggregator) Eval(_ *environment.Environment) (types.Value, error) {
	if s.SumF != nil {
		return types.NewDoublePrevisionValue(*s.SumF), nil
	}
	if s.SumI != nil {
		return types.NewBigintValue(*s.SumI), nil
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

// Eval extracts the average value from the given object and returns it.
func (s *Avg) Eval(env *environment.Environment) (types.Value, error) {
	r, ok := env.GetRow()
	if !ok {
		return nil, errors.New("misuse of aggregation function AVG()")
	}

	return r.Get(s.String())
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
	if err != nil && !errors.Is(err, types.ErrColumnNotFound) {
		return err
	}

	switch v.Type() {
	case types.TypeInteger, types.TypeBigint:
		s.Avg += float64(types.AsInt64(v))
	case types.TypeDoublePrecision:
		s.Avg += types.AsFloat64(v)
	default:
		return nil
	}
	s.Counter++

	return nil
}

// Eval returns the aggregated average as a double.
func (s *AvgAggregator) Eval(_ *environment.Environment) (types.Value, error) {
	if s.Counter == 0 {
		return types.NewDoublePrevisionValue(0), nil
	}

	return types.NewDoublePrevisionValue(s.Avg / float64(s.Counter)), nil
}

func (s *AvgAggregator) String() string {
	return s.Fn.String()
}

// Len represents the len() function.
// It returns the length of string, array or row.
// For other types len() returns NULL.
type Len struct {
	Expr expr.Expr
}

// Eval extracts the average value from the given object and returns it.
func (s *Len) Eval(env *environment.Environment) (types.Value, error) {
	val, err := s.Expr.Eval(env)
	if err != nil {
		return nil, err
	}
	var length int
	switch val.Type() {
	case types.TypeText:
		length = len(types.AsString(val))
	default:
		return types.NewNullValue(), nil
	}

	return types.NewBigintValue(int64(length)), nil
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (s *Len) IsEqual(other expr.Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(*Len)
	if !ok {
		return false
	}

	return expr.Equal(s.Expr, o.Expr)
}

func (s *Len) Params() []expr.Expr { return []expr.Expr{s.Expr} }

// String returns the literal representation of len.
func (s *Len) String() string {
	return fmt.Sprintf("LEN(%v)", s.Expr)
}

type Coalesce struct {
	Exprs []expr.Expr
}

func (c *Coalesce) Eval(e *environment.Environment) (types.Value, error) {
	for _, exp := range c.Exprs {
		v, err := exp.Eval(e)
		if err != nil {
			return nil, err
		}
		if v.Type() != types.TypeNull {
			return v, nil
		}
	}
	return nil, nil
}

func (c *Coalesce) String() string {
	return fmt.Sprintf("COALESCE(%v)", c.Exprs)
}

func (c *Coalesce) Params() []expr.Expr {
	return c.Exprs
}

type Now struct{}

func (n *Now) Eval(env *environment.Environment) (types.Value, error) {
	tx := env.GetTx()
	if tx == nil {
		return nil, errors.New("misuse of NOW()")
	}

	return types.NewTimestampValue(tx.TxStart.UTC()), nil
}

func (n *Now) IsEqual(other expr.Expr) bool {
	if other == nil {
		return false
	}

	_, ok := other.(*Now)
	return ok
}

func (n *Now) Params() []expr.Expr { return nil }

func (n *Now) String() string {
	return "NOW()"
}

type NextVal struct {
	Expr expr.Expr
}

func (t *NextVal) Eval(env *environment.Environment) (types.Value, error) {
	seqNameV, err := t.Expr.Eval(env)
	if err != nil {
		return nil, err
	}
	if seqNameV.Type() != types.TypeText {
		return nil, fmt.Errorf("nextval argument must be a string")
	}

	tx := env.GetTx()
	if tx == nil {
		return types.NewNullValue(), fmt.Errorf(`nextval cannot be evaluated`)
	}

	seq, err := tx.Catalog.GetSequence(types.AsString(seqNameV))
	if err != nil {
		return types.NewNullValue(), err
	}

	i, err := seq.Next(tx)
	if err != nil {
		return types.NewNullValue(), err
	}

	return types.NewBigintValue(i), nil
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (t *NextVal) IsEqual(other expr.Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(*NextVal)
	if !ok {
		return false
	}

	return expr.Equal(t.Expr, o.Expr)
}

func (t *NextVal) Params() []expr.Expr { return []expr.Expr{t.Expr} }

func (t *NextVal) String() string {
	return fmt.Sprintf("nextval(%v)", t.Expr)
}
