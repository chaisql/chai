package expr

import (
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/sql/scanner"
)

type addOp struct {
	*simpleOperator
}

// Add creates an expression thats evaluates to the result of a + b.
func Add(a, b Expr) Expr {
	return &addOp{&simpleOperator{a, b, scanner.ADD}}
}

func (op addOp) Eval(ctx EvalStack) (document.Value, error) {
	a, b, err := op.simpleOperator.eval(ctx)
	if err != nil {
		return nullLitteral, err
	}

	return a.Add(b)
}

type subOp struct {
	*simpleOperator
}

// Sub creates an expression thats evaluates to the result of a - b.
func Sub(a, b Expr) Expr {
	return &subOp{&simpleOperator{a, b, scanner.SUB}}
}

func (op subOp) Eval(ctx EvalStack) (document.Value, error) {
	a, b, err := op.simpleOperator.eval(ctx)
	if err != nil {
		return nullLitteral, err
	}

	return a.Sub(b)
}

type mulOp struct {
	*simpleOperator
}

// Mul creates an expression thats evaluates to the result of a * b.
func Mul(a, b Expr) Expr {
	return &mulOp{&simpleOperator{a, b, scanner.MUL}}
}

func (op mulOp) Eval(ctx EvalStack) (document.Value, error) {
	a, b, err := op.simpleOperator.eval(ctx)
	if err != nil {
		return nullLitteral, err
	}

	return a.Mul(b)
}

type divOp struct {
	*simpleOperator
}

// Div creates an expression thats evaluates to the result of a / b.
func Div(a, b Expr) Expr {
	return &divOp{&simpleOperator{a, b, scanner.DIV}}
}

func (op divOp) Eval(ctx EvalStack) (document.Value, error) {
	a, b, err := op.simpleOperator.eval(ctx)
	if err != nil {
		return nullLitteral, err
	}

	return a.Div(b)
}

type modOp struct {
	*simpleOperator
}

// Mod creates an expression thats evaluates to the result of a % b.
func Mod(a, b Expr) Expr {
	return &modOp{&simpleOperator{a, b, scanner.MOD}}
}

func (op modOp) Eval(ctx EvalStack) (document.Value, error) {
	a, b, err := op.simpleOperator.eval(ctx)
	if err != nil {
		return nullLitteral, err
	}

	return a.Mod(b)
}

type bitwiseAndOp struct {
	*simpleOperator
}

// BitwiseAnd creates an expression thats evaluates to the result of a & b.
func BitwiseAnd(a, b Expr) Expr {
	return &bitwiseAndOp{&simpleOperator{a, b, scanner.ADD}}
}

func (op bitwiseAndOp) Eval(ctx EvalStack) (document.Value, error) {
	a, b, err := op.simpleOperator.eval(ctx)
	if err != nil {
		return nullLitteral, err
	}

	return a.BitwiseAnd(b)
}

type bitwiseOrOp struct {
	*simpleOperator
}

// BitwiseOr creates an expression thats evaluates to the result of a & b.
func BitwiseOr(a, b Expr) Expr {
	return &bitwiseOrOp{&simpleOperator{a, b, scanner.ADD}}
}

func (op bitwiseOrOp) Eval(ctx EvalStack) (document.Value, error) {
	a, b, err := op.simpleOperator.eval(ctx)
	if err != nil {
		return nullLitteral, err
	}

	return a.BitwiseOr(b)
}

type bitwiseXorOp struct {
	*simpleOperator
}

// BitwiseXor creates an expression thats evaluates to the result of a & b.
func BitwiseXor(a, b Expr) Expr {
	return &bitwiseXorOp{&simpleOperator{a, b, scanner.ADD}}
}

func (op bitwiseXorOp) Eval(ctx EvalStack) (document.Value, error) {
	a, b, err := op.simpleOperator.eval(ctx)
	if err != nil {
		return nullLitteral, err
	}

	return a.BitwiseXor(b)
}
