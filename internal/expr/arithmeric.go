package expr

import (
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/sql/scanner"
	"github.com/chaisql/chai/internal/types"
)

// IsArithmeticOperator returns true if e is one of
// +, -, *, /, %, &, |, or ^ operators.
func IsArithmeticOperator(op Operator) bool {
	_, ok := op.(*arithmeticOperator)
	return ok
}

type arithmeticOperator struct {
	*simpleOperator
}

func (op *arithmeticOperator) Eval(env *environment.Environment) (types.Value, error) {
	return op.simpleOperator.eval(env, func(va, vb types.Value) (types.Value, error) {
		a, ok := va.(types.Numeric)
		if !ok {
			return NullLiteral, nil
		}

		b, ok := vb.(types.Numeric)
		if !ok {
			return NullLiteral, nil
		}

		switch op.simpleOperator.Tok {
		case scanner.ADD:
			return a.Add(b)
		case scanner.SUB:
			return a.Sub(b)
		case scanner.MUL:
			return a.Mul(b)
		case scanner.DIV:
			return a.Div(b)
		case scanner.MOD:
			return a.Mod(b)
		}

		ia, ok := a.(types.Integral)
		if !ok {
			return NullLiteral, nil
		}

		_, ok = b.(types.Integral)
		if !ok {
			return NullLiteral, nil
		}

		switch op.simpleOperator.Tok {
		case scanner.BITWISEAND:
			return ia.BitwiseAnd(b)
		case scanner.BITWISEOR:
			return ia.BitwiseOr(b)
		case scanner.BITWISEXOR:
			return ia.BitwiseXor(b)
		}

		panic("unknown arithmetic token")
	})
}

// Add creates an expression thats evaluates to the result of a + b.
func Add(a, b Expr) Expr {
	return &arithmeticOperator{&simpleOperator{a, b, scanner.ADD}}
}

// Sub creates an expression thats evaluates to the result of a - b.
func Sub(a, b Expr) Expr {
	return &arithmeticOperator{&simpleOperator{a, b, scanner.SUB}}
}

// Mul creates an expression thats evaluates to the result of a * b.
func Mul(a, b Expr) Expr {
	return &arithmeticOperator{&simpleOperator{a, b, scanner.MUL}}
}

// Div creates an expression thats evaluates to the result of a / b.
func Div(a, b Expr) Expr {
	return &arithmeticOperator{&simpleOperator{a, b, scanner.DIV}}
}

// Mod creates an expression thats evaluates to the result of a % b.
func Mod(a, b Expr) Expr {
	return &arithmeticOperator{&simpleOperator{a, b, scanner.MOD}}
}

// BitwiseAnd creates an expression thats evaluates to the result of a & b.
func BitwiseAnd(a, b Expr) Expr {
	return &arithmeticOperator{&simpleOperator{a, b, scanner.BITWISEAND}}
}

// BitwiseOr creates an expression thats evaluates to the result of a | b.
func BitwiseOr(a, b Expr) Expr {
	return &arithmeticOperator{&simpleOperator{a, b, scanner.BITWISEOR}}
}

// BitwiseXor creates an expression thats evaluates to the result of a ^ b.
func BitwiseXor(a, b Expr) Expr {
	return &arithmeticOperator{&simpleOperator{a, b, scanner.BITWISEXOR}}
}
