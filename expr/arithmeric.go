package expr

import (
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/scanner"
)

// IsArithmeticOperator returns true if e is one of
// +, -, *, /, %, &, |, or ^ operators.
func IsArithmeticOperator(op Operator) bool {
	switch op.(type) {
	case *addOp, *subOp, *mulOp, *divOp, *modOp,
		*bitwiseAndOp, *bitwiseOrOp, *bitwiseXorOp:
		return true
	}

	return false
}

type addOp struct {
	*simpleOperator
}

// Add creates an expression thats evaluates to the result of a + b.
func Add(a, b Expr) Expr {
	return &addOp{&simpleOperator{a, b, scanner.ADD}}
}

func (op addOp) Eval(env *Environment) (document.Value, error) {
	return op.simpleOperator.eval(env, func(a, b document.Value) (document.Value, error) { return a.Add(b) })
}

type subOp struct {
	*simpleOperator
}

// Sub creates an expression thats evaluates to the result of a - b.
func Sub(a, b Expr) Expr {
	return &subOp{&simpleOperator{a, b, scanner.SUB}}
}

func (op subOp) Eval(env *Environment) (document.Value, error) {
	return op.simpleOperator.eval(env, func(a, b document.Value) (document.Value, error) { return a.Sub(b) })
}

type mulOp struct {
	*simpleOperator
}

// Mul creates an expression thats evaluates to the result of a * b.
func Mul(a, b Expr) Expr {
	return &mulOp{&simpleOperator{a, b, scanner.MUL}}
}

func (op mulOp) Eval(env *Environment) (document.Value, error) {
	return op.simpleOperator.eval(env, func(a, b document.Value) (document.Value, error) { return a.Mul(b) })
}

type divOp struct {
	*simpleOperator
}

// Div creates an expression thats evaluates to the result of a / b.
func Div(a, b Expr) Expr {
	return &divOp{&simpleOperator{a, b, scanner.DIV}}
}

func (op divOp) Eval(env *Environment) (document.Value, error) {
	return op.simpleOperator.eval(env, func(a, b document.Value) (document.Value, error) { return a.Div(b) })
}

type modOp struct {
	*simpleOperator
}

// Mod creates an expression thats evaluates to the result of a % b.
func Mod(a, b Expr) Expr {
	return &modOp{&simpleOperator{a, b, scanner.MOD}}
}

func (op modOp) Eval(env *Environment) (document.Value, error) {
	return op.simpleOperator.eval(env, func(a, b document.Value) (document.Value, error) { return a.Mod(b) })
}

type bitwiseAndOp struct {
	*simpleOperator
}

// BitwiseAnd creates an expression thats evaluates to the result of a & b.
func BitwiseAnd(a, b Expr) Expr {
	return &bitwiseAndOp{&simpleOperator{a, b, scanner.BITWISEAND}}
}

func (op bitwiseAndOp) Eval(env *Environment) (document.Value, error) {
	return op.simpleOperator.eval(env, func(a, b document.Value) (document.Value, error) { return a.BitwiseAnd(b) })
}

type bitwiseOrOp struct {
	*simpleOperator
}

// BitwiseOr creates an expression thats evaluates to the result of a | b.
func BitwiseOr(a, b Expr) Expr {
	return &bitwiseOrOp{&simpleOperator{a, b, scanner.BITWISEOR}}
}

func (op bitwiseOrOp) Eval(env *Environment) (document.Value, error) {
	return op.simpleOperator.eval(env, func(a, b document.Value) (document.Value, error) { return a.BitwiseOr(b) })
}

type bitwiseXorOp struct {
	*simpleOperator
}

// BitwiseXor creates an expression thats evaluates to the result of a ^ b.
func BitwiseXor(a, b Expr) Expr {
	return &bitwiseXorOp{&simpleOperator{a, b, scanner.BITWISEXOR}}
}

func (op bitwiseXorOp) Eval(env *Environment) (document.Value, error) {
	return op.simpleOperator.eval(env, func(a, b document.Value) (document.Value, error) { return a.BitwiseXor(b) })
}
