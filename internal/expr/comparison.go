package expr

import (
	"fmt"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/sql/scanner"
	"github.com/chaisql/chai/internal/types"
)

// A cmpOp is a comparison operator.
type cmpOp struct {
	*simpleOperator
}

// newCmpOp creates a comparison operator.
func newCmpOp(a, b Expr, t scanner.Token) *cmpOp {
	return &cmpOp{&simpleOperator{a, b, t}}
}

// Eval compares a and b together using the operator specified when constructing the CmpOp
// and returns the result of the comparison.
// Comparing with NULL always evaluates to NULL.
func (op *cmpOp) Eval(env *environment.Environment) (types.Value, error) {
	return op.simpleOperator.eval(env, func(a, b types.Value) (types.Value, error) {
		if a.Type() == types.TypeNull || b.Type() == types.TypeNull {
			return NullLiteral, nil
		}

		ok, err := op.compare(a, b)
		if ok {
			return TrueLiteral, err
		}

		return FalseLiteral, err
	})
}

func (op *cmpOp) compare(l, r types.Value) (bool, error) {
	switch op.Tok {
	case scanner.EQ:
		return l.EQ(r)
	case scanner.NEQ:
		eq, err := l.EQ(r)
		if err != nil {
			return false, err
		}
		return !eq, nil
	case scanner.GT:
		return l.GT(r)
	case scanner.GTE:
		return l.GTE(r)
	case scanner.LT:
		return l.LT(r)
	case scanner.LTE:
		return l.LTE(r)
	default:
		panic(fmt.Sprintf("unknown token %v", op.Tok))
	}
}

func (op *cmpOp) Clone() Expr {
	return &cmpOp{op.simpleOperator.Clone()}
}

// Eq creates an expression that returns true if a equals b.
func Eq(a, b Expr) Expr {
	return newCmpOp(a, b, scanner.EQ)
}

// Neq creates an expression that returns true if a equals b.
func Neq(a, b Expr) Expr {
	return newCmpOp(a, b, scanner.NEQ)
}

// Gt creates an expression that returns true if a is greater than b.
func Gt(a, b Expr) Expr {
	return newCmpOp(a, b, scanner.GT)
}

// Gte creates an expression that returns true if a is greater than or equal to b.
func Gte(a, b Expr) Expr {
	return newCmpOp(a, b, scanner.GTE)
}

// Lt creates an expression that returns true if a is lesser than b.
func Lt(a, b Expr) Expr {
	return newCmpOp(a, b, scanner.LT)
}

// Lte creates an expression that returns true if a is lesser than or equal to b.
func Lte(a, b Expr) Expr {
	return newCmpOp(a, b, scanner.LTE)
}

type BetweenOperator struct {
	*simpleOperator
	X Expr
}

// Between returns a function that creates a BETWEEN operator that
// returns true if x is between a and b.
func Between(a Expr) func(x, b Expr) Expr {
	return func(x, b Expr) Expr {
		return &BetweenOperator{&simpleOperator{a, b, scanner.BETWEEN}, x}
	}
}

func (op *BetweenOperator) Clone() Expr {
	return &BetweenOperator{
		op.simpleOperator.Clone(),
		Clone(op.X),
	}
}

func (op *BetweenOperator) Eval(env *environment.Environment) (types.Value, error) {
	x, err := op.X.Eval(env)
	if err != nil {
		return FalseLiteral, err
	}

	return op.simpleOperator.eval(env, func(a, b types.Value) (types.Value, error) {
		if a.Type() == types.TypeNull || b.Type() == types.TypeNull || x.Type() == types.TypeNull {
			return NullLiteral, nil
		}

		ok, err := x.Between(a, b)
		if err != nil {
			return NullLiteral, err
		}

		if ok {
			return TrueLiteral, nil
		}

		return FalseLiteral, nil
	})
}

func (op *BetweenOperator) String() string {
	return fmt.Sprintf("%v BETWEEN %v AND %v", op.X, op.a, op.b)
}

// IsComparisonOperator returns true if e is one of
// =, !=, >, >=, <, <=, IS, IS NOT, IN, or NOT IN operators.
func IsComparisonOperator(op Operator) bool {
	switch op.(type) {
	case *cmpOp, *IsOperator, *IsNotOperator, *InOperator, *NotInOperator, *LikeOperator, *NotLikeOperator, *BetweenOperator:
		return true
	}

	return false
}

type InOperator struct {
	a  Expr
	b  Expr
	op scanner.Token
}

// In creates an expression that evaluates to the result of a IN b.
func In(a Expr, b Expr) Expr {
	return &InOperator{a, b, scanner.IN}
}

func (op *InOperator) Clone() Expr {
	return &InOperator{
		Clone(op.a),
		Clone(op.b),
		op.op,
	}
}

func (op *InOperator) Precedence() int {
	return op.op.Precedence()
}

func (op *InOperator) LeftHand() Expr {
	return op.a
}

func (op *InOperator) RightHand() Expr {
	return op.b
}

func (op *InOperator) SetLeftHandExpr(a Expr) {
	op.a = a
}

func (op *InOperator) SetRightHandExpr(b Expr) {}

func (op *InOperator) Token() scanner.Token {
	return op.op
}

func (op *InOperator) String() string {
	return fmt.Sprintf("%v IN %v", op.a, op.b)
}

func (op *InOperator) Eval(env *environment.Environment) (types.Value, error) {
	a, err := op.validateLeftExpression(op.a)
	if err != nil {
		return NullLiteral, err
	}

	b, err := op.validateRightExpression(op.b)
	if err != nil {
		return NullLiteral, err
	}

	va, err := a.Eval(env)
	if err != nil {
		return NullLiteral, err
	}

	if va.Type() == types.TypeNull {
		return NullLiteral, nil
	}

	for _, bb := range b {
		v, err := bb.Eval(env)
		if err != nil {
			return NullLiteral, err
		}

		ok, err := va.EQ(v)
		if err != nil {
			return NullLiteral, err
		}

		if ok {
			return TrueLiteral, nil
		}
	}

	return FalseLiteral, nil
}

func (op *InOperator) validateLeftExpression(a Expr) (Expr, error) {
	switch t := a.(type) {
	case Parentheses:
		return op.validateLeftExpression(t.E)
	case *Column:
		return a, nil
	case LiteralValue:
		return a, nil
	}

	return nil, fmt.Errorf("invalid left expression for IN operator: %v", a)
}

func (op *InOperator) validateRightExpression(b Expr) (LiteralExprList, error) {
	switch t := b.(type) {
	case Parentheses:
		return LiteralExprList{b.(Parentheses).E}, nil
	case LiteralExprList:
		return t, nil
	}

	return nil, fmt.Errorf("invalid right expression for IN operator: %v", b)
}

type NotInOperator struct {
	*InOperator
}

// NotIn creates an expression that evaluates to the result of a NOT IN b.
func NotIn(a Expr, b Expr) Expr {
	return &NotInOperator{&InOperator{a, b, scanner.NIN}}
}

func (op *NotInOperator) Clone() Expr {
	return &NotInOperator{
		op.InOperator.Clone().(*InOperator),
	}
}

func (op *NotInOperator) Eval(env *environment.Environment) (types.Value, error) {
	return invertBoolResult(op.InOperator.Eval)(env)
}

func (op *NotInOperator) String() string {
	return fmt.Sprintf("%v NOT IN %v", op.a, op.b)
}

type IsOperator struct {
	*simpleOperator
}

// Is creates an expression that evaluates to the result of a IS b.
func Is(a, b Expr) Expr {
	return &IsOperator{&simpleOperator{a, b, scanner.IN}}
}

func (op *IsOperator) Clone() Expr {
	return &IsOperator{
		op.simpleOperator.Clone(),
	}
}

func (op *IsOperator) Eval(env *environment.Environment) (types.Value, error) {
	return op.simpleOperator.eval(env, func(a, b types.Value) (types.Value, error) {
		ok, err := a.EQ(b)
		if err != nil {
			return NullLiteral, err
		}
		if ok {
			return TrueLiteral, nil
		}

		return FalseLiteral, nil
	})
}

type IsNotOperator struct {
	*simpleOperator
}

// IsNot creates an expression that evaluates to the result of a IS NOT b.
func IsNot(a, b Expr) Expr {
	return &IsNotOperator{&simpleOperator{a, b, scanner.ISN}}
}

func (op *IsNotOperator) Clone() Expr {
	return &IsNotOperator{
		op.simpleOperator.Clone(),
	}
}

func (op *IsNotOperator) Eval(env *environment.Environment) (types.Value, error) {
	return op.simpleOperator.eval(env, func(a, b types.Value) (types.Value, error) {
		eq, err := a.EQ(b)
		if err != nil {
			return NullLiteral, err
		}
		if !eq {
			return TrueLiteral, nil
		}

		return FalseLiteral, nil
	})
}

func (op *IsNotOperator) String() string {
	return fmt.Sprintf("%v IS NOT %v", op.a, op.b)
}
