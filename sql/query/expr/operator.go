package expr

import (
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/scanner"
)

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

// OperatorIsIndexCompatible returns whether the operator can be used to read from an index.
func OperatorIsIndexCompatible(op Operator) bool {
	switch op.(type) {
	case *EqOperator, *GtOperator, *GteOperator, *LtOperator, *LteOperator, *InOperator:
		return true
	}

	return false
}
