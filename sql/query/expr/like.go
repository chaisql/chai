package expr

import (
	"errors"
	"fmt"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/glob"
	"github.com/genjidb/genji/sql/scanner"
)

func like(pattern, text string) bool {
	return glob.MatchLike(pattern, text)
}

type likeOp struct {
	*simpleOperator
}

// Like creates an expression that evaluates to the result of a LIKE b.
func Like(a, b Expr) Expr {
	return &likeOp{&simpleOperator{a, b, scanner.LIKE}}
}

func (op likeOp) Eval(env *Environment) (document.Value, error) {
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

func (op likeOp) String() string {
	return fmt.Sprintf("%v LIKE %v", op.a, op.b)
}

type notLikeOp struct {
	likeOp
}

// NotLike creates an expression that evaluates to the result of a NOT LIKE b.
func NotLike(a, b Expr) Expr {
	return &notLikeOp{likeOp{&simpleOperator{a, b, scanner.LIKE}}}
}

func (op notLikeOp) Eval(env *Environment) (document.Value, error) {
	return invertBoolResult(op.likeOp.Eval)(env)
}

func (op notLikeOp) String() string {
	return fmt.Sprintf("%v NOT LIKE %v", op.a, op.b)
}
