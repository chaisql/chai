package expr

import (
	"errors"
	"fmt"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/query/glob"
	"github.com/genjidb/genji/sql/scanner"
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

func (op *LikeOperator) Eval(env *Environment) (document.Value, error) {
	a, b, err := op.simpleOperator.eval(env)
	if err != nil {
		return nullLitteral, err
	}

	if a.Type != document.TextValue || b.Type != document.TextValue {
		return nullLitteral, errors.New("LIKE operator takes a text")
	}

	if like(b.V.(string), a.V.(string)) {
		return trueLitteral, nil
	}

	return falseLitteral, nil
}

func (op *LikeOperator) String() string {
	return fmt.Sprintf("%v LIKE %v", op.a, op.b)
}

type NotLikeOperator struct {
	LikeOperator
}

// NotLike creates an expression that evaluates to the result of a NOT LIKE b.
func NotLike(a, b Expr) Expr {
	return &NotLikeOperator{LikeOperator{&simpleOperator{a, b, scanner.LIKE}}}
}

func (op *NotLikeOperator) Eval(env *Environment) (document.Value, error) {
	return invertBoolResult(op.LikeOperator.Eval)(env)
}

func (op *NotLikeOperator) String() string {
	return fmt.Sprintf("%v NOT LIKE %v", op.a, op.b)
}
