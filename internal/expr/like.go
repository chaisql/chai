package expr

import (
	"fmt"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr/glob"
	"github.com/chaisql/chai/internal/sql/scanner"
	"github.com/chaisql/chai/internal/types"
)

func like(pattern, text string) bool {
	return glob.MatchLike(pattern, text)
}

type LikeOperator struct {
	*simpleOperator
}

// Like creates an expression that evaluates to the result of a LIKE b.
func Like(a, b Expr) Expr {
	return &LikeOperator{&simpleOperator{a, b, scanner.LIKE}}
}

func (op *LikeOperator) Eval(env *environment.Environment) (types.Value, error) {
	return op.simpleOperator.eval(env, func(a, b types.Value) (types.Value, error) {
		if a.Type() != types.TypeText || b.Type() != types.TypeText {
			return NullLiteral, nil
		}

		if like(types.AsString(b), types.AsString(a)) {
			return TrueLiteral, nil
		}

		return FalseLiteral, nil
	})
}

func (op *LikeOperator) String() string {
	return fmt.Sprintf("%v LIKE %v", op.a, op.b)
}

type NotLikeOperator struct {
	*LikeOperator
}

// NotLike creates an expression that evaluates to the result of a NOT LIKE b.
func NotLike(a, b Expr) Expr {
	return &NotLikeOperator{&LikeOperator{&simpleOperator{a, b, scanner.NLIKE}}}
}

func (op *NotLikeOperator) Eval(env *environment.Environment) (types.Value, error) {
	return invertBoolResult(op.LikeOperator.Eval)(env)
}

func (op *NotLikeOperator) String() string {
	return fmt.Sprintf("%v NOT LIKE %v", op.a, op.b)
}
