package expr

import (
	"fmt"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/types"
)

var (
	TrueLiteral  = types.NewBooleanValue(true)
	FalseLiteral = types.NewBooleanValue(false)
	NullLiteral  = types.NewNullValue()
)

// An Expr evaluates to a value.
type Expr interface {
	Eval(*environment.Environment) (types.Value, error)
	String() string
}

type isEqualer interface {
	IsEqual(Expr) bool
}

// Equal reports whether a and b are equal by first calling IsEqual
// if they have an IsEqual method with this signature:
//
//	IsEqual(Expr) bool
//
// If not, it returns whether a and b values are equal.
func Equal(a, b Expr) bool {
	if aa, ok := a.(isEqualer); ok {
		return aa.IsEqual(b)
	}

	if bb, ok := b.(isEqualer); ok {
		return bb.IsEqual(a)
	}

	return a == b
}

// Parentheses is a special expression which turns
// any sub-expression as unary.
// It hides the underlying operator, if any, from the parser
// so that it doesn't get reordered by precedence.
type Parentheses struct {
	E Expr
}

// Eval calls the underlying expression Eval method.
func (p Parentheses) Eval(env *environment.Environment) (types.Value, error) {
	return p.E.Eval(env)
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (p Parentheses) IsEqual(other Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(Parentheses)
	if !ok {
		return false
	}

	return Equal(p.E, o.E)
}

func (p Parentheses) String() string {
	return fmt.Sprintf("(%v)", p.E)
}

func invertBoolResult(f func(env *environment.Environment) (types.Value, error)) func(env *environment.Environment) (types.Value, error) {
	return func(env *environment.Environment) (types.Value, error) {
		v, err := f(env)

		if err != nil {
			return v, err
		}
		if v == TrueLiteral {
			return FalseLiteral, nil
		}
		if v == FalseLiteral {
			return TrueLiteral, nil
		}
		return v, nil
	}
}

// NamedExpr is an expression with a name.
type NamedExpr struct {
	Expr

	ExprName string
}

func (e *NamedExpr) String() string {
	return e.ExprName
}

// A Function is an expression whose evaluation calls a function previously defined.
type Function interface {
	Expr

	// Params returns the list of parameters this function has received.
	Params() []Expr
}

// An Aggregator is an expression that aggregates objects into one result.
type Aggregator interface {
	Expr

	Aggregate(env *environment.Environment) error
}

// An AggregatorBuilder is a type that can create aggregators.
type AggregatorBuilder interface {
	Expr

	Aggregator() Aggregator
}

func Walk(e Expr, fn func(Expr) bool) bool {
	if e == nil {
		return true
	}
	if !fn(e) {
		return false
	}

	switch t := e.(type) {
	case Operator:
		if !Walk(t.LeftHand(), fn) {
			return false
		}
		if !Walk(t.RightHand(), fn) {
			return false
		}
	case *NamedExpr:
		return Walk(t.Expr, fn)
	case Function:
		for _, p := range t.Params() {
			if !Walk(p, fn) {
				return false
			}
		}
	}

	return true
}
