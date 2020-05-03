package expr

import (
	"fmt"

	"github.com/asdine/genji/document"
	"github.com/asdine/genji/sql/scanner"
)

// A cmpOp is a comparison operator.
type cmpOp struct {
	*simpleOperator
}

// newCmpOp creates a comparison operator.
func newCmpOp(a, b Expr, t scanner.Token) Operator {
	return cmpOp{&simpleOperator{a, b, t}}
}

// Eq creates an expression that returns true if a equals b.
func Eq(a, b Expr) Expr {
	return cmpOp{&simpleOperator{a, b, scanner.EQ}}
}

// Neq creates an expression that returns true if a equals b.
func Neq(a, b Expr) Expr {
	return cmpOp{&simpleOperator{a, b, scanner.NEQ}}
}

// Gt creates an expression that returns true if a is greater than b.
func Gt(a, b Expr) Expr {
	return cmpOp{&simpleOperator{a, b, scanner.GT}}
}

// Gte creates an expression that returns true if a is greater than or equal to b.
func Gte(a, b Expr) Expr {
	return cmpOp{&simpleOperator{a, b, scanner.GTE}}
}

// Lt creates an expression that returns true if a is lesser than b.
func Lt(a, b Expr) Expr {
	return cmpOp{&simpleOperator{a, b, scanner.LT}}
}

// Lte creates an expression that returns true if a is lesser than or equal to b.
func Lte(a, b Expr) Expr {
	return cmpOp{&simpleOperator{a, b, scanner.LTE}}
}

// Eval compares a and b together using the operator specified when constructing the CmpOp
// and returns the result of the comparison.
// Comparing with NULL always evaluates to NULL.
func (op cmpOp) Eval(ctx EvalStack) (document.Value, error) {
	v1, v2, err := op.simpleOperator.eval(ctx)
	if err != nil {
		return falseLitteral, err
	}

	if v1.Type == document.NullValue || v2.Type == document.NullValue {
		return nullLitteral, nil
	}

	ok, err := op.compare(v1, v2)
	if ok {
		return trueLitteral, err
	}

	return falseLitteral, err
}

func (op cmpOp) compare(l, r document.Value) (bool, error) {
	switch op.Tok {
	case scanner.EQ:
		return l.IsEqual(r)
	case scanner.NEQ:
		return l.IsNotEqual(r)
	case scanner.GT:
		return l.IsGreaterThan(r)
	case scanner.GTE:
		return l.IsGreaterThanOrEqual(r)
	case scanner.LT:
		return l.IsLesserThan(r)
	case scanner.LTE:
		return l.IsLesserThanOrEqual(r)
	default:
		panic(fmt.Sprintf("unknown token %v", op.Tok))
	}
}

// IsComparisonOperator returns true if e is one of
// =, !=, >, >=, <, <=, IS, IS NOT, IN, or NOT IN operators.
func IsComparisonOperator(op Operator) bool {
	_, ok := op.(*cmpOp)
	return ok
}

// IsAndOperator reports if e is the AND operator.
func IsAndOperator(op Operator) bool {
	_, ok := op.(*AndOp)
	return ok
}

// IsOrOperator reports if e is the OR operator.
func IsOrOperator(e Expr) bool {
	_, ok := e.(*OrOp)
	return ok
}

type inOp struct {
	*simpleOperator
}

// In creates an expression that evaluates to the result of a IN b.
func In(a, b Expr) Expr {
	return &inOp{&simpleOperator{a, b, scanner.IN}}
}

func (op inOp) Eval(ctx EvalStack) (document.Value, error) {
	a, b, err := op.simpleOperator.eval(ctx)
	if err != nil {
		return nullLitteral, err
	}

	if a.Type == document.NullValue || b.Type == document.NullValue {
		return nullLitteral, nil
	}

	if b.Type != document.ArrayValue {
		return falseLitteral, nil
	}

	arr, err := b.ConvertToArray()
	if err != nil {
		return nullLitteral, err
	}

	ok, err := document.ArrayContains(arr, a)
	if err != nil {
		return nullLitteral, err
	}

	if ok {
		return trueLitteral, nil
	}
	return falseLitteral, nil
}

type notInOp struct {
	Expr
}

// NotIn creates an expression that evaluates to the result of a NOT IN b.
func NotIn(a, b Expr) Expr {
	return &notInOp{In(a, b)}
}

func (op notInOp) Eval(ctx EvalStack) (document.Value, error) {
	v, err := op.Expr.Eval(ctx)
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

type isOp struct {
	*simpleOperator
}

// Is creates an expression that evaluates to the result of a IS b.
func Is(a, b Expr) Expr {
	return &isOp{&simpleOperator{a, b, scanner.IN}}
}

func (op isOp) Eval(ctx EvalStack) (document.Value, error) {
	a, b, err := op.simpleOperator.eval(ctx)
	if err != nil {
		return nullLitteral, err
	}

	ok, err := a.IsEqual(b)
	if err != nil {
		return nullLitteral, err
	}
	if ok {
		return trueLitteral, nil
	}

	return falseLitteral, nil
}

type isNotOp struct {
	*simpleOperator
}

// IsNot creates an expression that evaluates to the result of a IS NOT b.
func IsNot(a, b Expr) Expr {
	return &isNotOp{&simpleOperator{a, b, scanner.IN}}
}

func (op isNotOp) Eval(ctx EvalStack) (document.Value, error) {
	a, b, err := op.simpleOperator.eval(ctx)
	if err != nil {
		return nullLitteral, err
	}

	ok, err := a.IsNotEqual(b)
	if err != nil {
		return nullLitteral, err
	}
	if ok {
		return trueLitteral, nil
	}

	return falseLitteral, nil
}
