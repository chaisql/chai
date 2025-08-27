package expr

import (
	"fmt"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/sql/scanner"
	"github.com/chaisql/chai/internal/types"
)

// AndOp is the And operator.
type AndOp struct {
	*simpleOperator
}

// And creates an expression that evaluates a And b And returns true if both are truthy.
func And(a, b Expr) Expr {
	return &AndOp{&simpleOperator{a, b, scanner.AND}}
}

func (op *AndOp) Clone() Expr {
	return &AndOp{
		simpleOperator: op.simpleOperator.Clone(),
	}
}

// Eval implements the Expr interface. It evaluates a and b and returns true if both evaluate
// to true.
func (op *AndOp) Eval(env *environment.Environment) (types.Value, error) {
	s, err := op.a.Eval(env)
	if err != nil {
		return FalseLiteral, err
	}
	isTruthy, err := types.IsTruthy(s)
	if !isTruthy || err != nil {
		return FalseLiteral, err
	}

	s, err = op.b.Eval(env)
	if err != nil {
		return FalseLiteral, err
	}
	isTruthy, err = types.IsTruthy(s)
	if !isTruthy || err != nil {
		return FalseLiteral, err
	}

	return TrueLiteral, nil
}

// OrOp is the Or operator.
type OrOp struct {
	*simpleOperator
}

// Or creates an expression that first evaluates a, returns true if truthy, then evaluates b, returns true if truthy Or false if falsy.
func Or(a, b Expr) Expr {
	return &OrOp{&simpleOperator{a, b, scanner.OR}}
}

func (op *OrOp) Clone() Expr {
	return &OrOp{
		simpleOperator: op.simpleOperator.Clone(),
	}
}

// Eval implements the Expr interface. It evaluates a and b and returns true if a or b evalutate
// to true.
func (op *OrOp) Eval(env *environment.Environment) (types.Value, error) {
	s, err := op.a.Eval(env)
	if err != nil {
		return FalseLiteral, err
	}
	isTruthy, err := types.IsTruthy(s)
	if err != nil {
		return FalseLiteral, err
	}
	if isTruthy {
		return TrueLiteral, nil
	}

	s, err = op.b.Eval(env)
	if err != nil {
		return FalseLiteral, err
	}
	isTruthy, err = types.IsTruthy(s)
	if err != nil {
		return FalseLiteral, err
	}
	if isTruthy {
		return TrueLiteral, nil
	}

	return FalseLiteral, nil
}

// NotOp is the NOT unary operator.
type NotOp struct {
	*simpleOperator
}

// Not creates an expression that returns true if e is falsy.
func Not(e Expr) Expr {
	return &NotOp{&simpleOperator{a: e}}
}

func (op *NotOp) Clone() Expr {
	return &NotOp{
		simpleOperator: op.simpleOperator.Clone(),
	}
}

// Eval implements the Expr interface. It evaluates e and returns true if b is falsy
func (op *NotOp) Eval(env *environment.Environment) (types.Value, error) {
	s, err := op.a.Eval(env)
	if err != nil {
		return FalseLiteral, err
	}

	isTruthy, err := types.IsTruthy(s)
	if err != nil {
		return FalseLiteral, err
	}
	if isTruthy {
		return FalseLiteral, nil
	}

	return TrueLiteral, nil
}

// String implements the fmt.Stringer interface.
func (op *NotOp) String() string {
	return fmt.Sprintf("NOT %v", op.a)
}
