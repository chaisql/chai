package expr

import (
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/sql/scanner"
	"github.com/genjidb/genji/internal/stringutil"
)

// AndOp is the And operator.
type AndOp struct {
	*simpleOperator
}

// And creates an expression that evaluates a And b And returns true if both are truthy.
func And(a, b Expr) Expr {
	return &AndOp{&simpleOperator{a, b, scanner.AND}}
}

// Eval implements the Expr interface. It evaluates a and b and returns true if both evaluate
// to true.
func (op *AndOp) Eval(env *environment.Environment) (document.Value, error) {
	s, err := op.a.Eval(env)
	if err != nil {
		return FalseLitteral, err
	}
	isTruthy, err := s.IsTruthy()
	if !isTruthy || err != nil {
		return FalseLitteral, err
	}

	s, err = op.b.Eval(env)
	if err != nil {
		return FalseLitteral, err
	}
	isTruthy, err = s.IsTruthy()
	if !isTruthy || err != nil {
		return FalseLitteral, err
	}

	return TrueLitteral, nil
}

// OrOp is the Or operator.
type OrOp struct {
	*simpleOperator
}

// Or creates an expression that first evaluates a, returns true if truthy, then evaluates b, returns true if truthy Or false if falsy.
func Or(a, b Expr) Expr {
	return &OrOp{&simpleOperator{a, b, scanner.OR}}
}

// Eval implements the Expr interface. It evaluates a and b and returns true if a or b evalutate
// to true.
func (op *OrOp) Eval(env *environment.Environment) (document.Value, error) {
	s, err := op.a.Eval(env)
	if err != nil {
		return FalseLitteral, err
	}
	isTruthy, err := s.IsTruthy()
	if err != nil {
		return FalseLitteral, err
	}
	if isTruthy {
		return TrueLitteral, nil
	}

	s, err = op.b.Eval(env)
	if err != nil {
		return FalseLitteral, err
	}
	isTruthy, err = s.IsTruthy()
	if err != nil {
		return FalseLitteral, err
	}
	if isTruthy {
		return TrueLitteral, nil
	}

	return FalseLitteral, nil
}

// NotOp is the NOT unary operator.
type NotOp struct {
	*simpleOperator
}

// Not creates an expression that returns true if e is falsy.
func Not(e Expr) Expr {
	return &NotOp{&simpleOperator{a: e}}
}

// Eval implements the Expr interface. It evaluates e and returns true if b is falsy
func (op *NotOp) Eval(env *environment.Environment) (document.Value, error) {
	s, err := op.a.Eval(env)
	if err != nil {
		return FalseLitteral, err
	}

	isTruthy, err := s.IsTruthy()
	if err != nil {
		return FalseLitteral, err
	}
	if isTruthy {
		return FalseLitteral, nil
	}

	return TrueLitteral, nil
}

// String implements the stringutil.Stringer interface.
func (op *NotOp) String() string {
	return stringutil.Sprintf("NOT %v", op.a)
}
