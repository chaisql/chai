package expr

import (
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/scanner"
)

var (
	trueLitteral  = document.NewBoolValue(true)
	falseLitteral = document.NewBoolValue(false)
	nullLitteral  = document.NewNullValue()
)

// An Expr evaluates to a value.
type Expr interface {
	Eval(*Environment) (document.Value, error)
}

type isEqualer interface {
	IsEqual(Expr) bool
}

// Equal reports whether a and b are equal by first calling IsEqual
// if they have an IsEqual method with this signature:
//   IsEqual(Expr) bool
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

type simpleOperator struct {
	a, b Expr
	Tok  scanner.Token
}

func (op simpleOperator) Precedence() int {
	return op.Tok.Precedence()
}

func (op simpleOperator) LeftHand() Expr {
	return op.a
}

func (op simpleOperator) RightHand() Expr {
	return op.b
}

func (op *simpleOperator) SetLeftHandExpr(a Expr) {
	op.a = a
}

func (op *simpleOperator) SetRightHandExpr(b Expr) {
	op.b = b
}

func (op *simpleOperator) Token() scanner.Token {
	return op.Tok
}

func (op *simpleOperator) eval(env *Environment) (document.Value, document.Value, error) {
	va, err := op.a.Eval(env)
	if err != nil {
		return nullLitteral, nullLitteral, err
	}

	vb, err := op.b.Eval(env)
	if err != nil {
		return nullLitteral, nullLitteral, err
	}

	return va, vb, nil
}

// Equal compares this expression with the other expression and returns
// true if they are equal.
func (op *simpleOperator) IsEqual(other Expr) bool {
	if other == nil {
		return false
	}

	oop, ok := other.(Operator)
	if !ok {
		return false
	}

	return op.Tok == oop.Token() &&
		Equal(op.a, oop.LeftHand()) &&
		Equal(op.b, oop.RightHand())
}

// An Operator is a binary expression that
// takes two operands and executes an operation on them.
type Operator interface {
	Expr

	Precedence() int
	LeftHand() Expr
	RightHand() Expr
	SetLeftHandExpr(Expr)
	SetRightHandExpr(Expr)
	Token() scanner.Token
}

// Parentheses is a special expression which turns
// any sub-expression as unary.
// It hides the underlying operator, if any, from the parser
// so that it doesn't get reordered by precedence.
type Parentheses struct {
	E Expr
}

// Eval calls the underlying expression Eval method.
func (p Parentheses) Eval(env *Environment) (document.Value, error) {
	return p.E.Eval(env)
}

func invertBoolResult(f func(env *Environment) (document.Value, error)) func(env *Environment) (document.Value, error) {
	return func(env *Environment) (document.Value, error) {
		v, err := f(env)

		if err != nil {
			return v, err
		}
		if v == trueLitteral {
			return falseLitteral, nil
		}
		if v == falseLitteral {
			return trueLitteral, nil
		}
		return v, nil
	}
}
