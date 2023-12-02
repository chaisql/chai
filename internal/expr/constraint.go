package expr

import (
	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

type ConstraintExpr struct {
	Expr Expr
}

func Constraint(e Expr) *ConstraintExpr {
	return &ConstraintExpr{
		Expr: e,
	}
}

func (t *ConstraintExpr) Eval(tx *database.Transaction, o types.Object) (types.Value, error) {
	var env environment.Environment
	env.Tx = tx
	env.SetRowFromObject(o)

	if t.Expr == nil {
		return NullLiteral, errors.New("missing expression")
	}

	return t.Expr.Eval(&env)
}

func (t *ConstraintExpr) String() string {
	return t.Expr.String()
}
