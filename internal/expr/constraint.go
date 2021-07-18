package expr

import (
	"errors"

	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/types"
)

type ConstraintExpr struct {
	Expr    Expr
	Catalog database.Catalog
}

func Constraint(e Expr) *ConstraintExpr {
	return &ConstraintExpr{
		Expr: e,
	}
}

func (t *ConstraintExpr) Eval(tx *database.Transaction) (types.Value, error) {
	var env environment.Environment
	env.Catalog = t.Catalog
	env.Tx = tx

	if t.Expr == nil {
		return NullLiteral, errors.New("missing expression")
	}

	return t.Expr.Eval(&env)
}

func (t *ConstraintExpr) Bind(catalog database.Catalog) {
	t.Catalog = catalog
}

func (t *ConstraintExpr) IsEqual(other database.TableExpression) bool {
	if t == nil {
		return other == nil
	}
	if other == nil {
		return false
	}
	o, ok := other.(*ConstraintExpr)
	if !ok {
		return false
	}
	return Equal(t.Expr, o.Expr)
}

func (t *ConstraintExpr) String() string {
	return t.Expr.String()
}
