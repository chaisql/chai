package expr

import (
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/sql/scanner"
	"github.com/genjidb/genji/types"
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
	return op.simpleOperator.eval(env, func(a, b types.Value) (types.Value, error) {
		switch op.simpleOperator.Tok {
		case scanner.ADD:
			return types.Add(a, b)
		case scanner.SUB:
			return types.Sub(a, b)
		case scanner.MUL:
			return types.Mul(a, b)
		case scanner.DIV:
			return types.Div(a, b)
		case scanner.MOD:
			return types.Mod(a, b)
		case scanner.BITWISEAND:
			return types.BitwiseAnd(a, b)
		case scanner.BITWISEOR:
			return types.BitwiseOr(a, b)
		case scanner.BITWISEXOR:
			return types.BitwiseXor(a, b)
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
