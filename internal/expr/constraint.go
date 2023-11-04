package expr

import (
	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/types"
)

type ConstraintExpr struct {
	Expr Expr
}

func Constraint(e Expr) *ConstraintExpr {
	return &ConstraintExpr{
		Expr: e,
	}
}

func (t *ConstraintExpr) Eval(tx *database.Transaction, d types.Document) (types.Value, error) {
	var env environment.Environment
	env.Tx = tx
	env.SetDocument(d)

	if t.Expr == nil {
		return NullLiteral, errors.New("missing expression")
	}

	return t.Expr.Eval(&env)
}

func (t *ConstraintExpr) String() string {
	return t.Expr.String()
}
