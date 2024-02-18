package expr

import (
	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/row"
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

func (t *ConstraintExpr) Eval(tx *database.Transaction, r row.Row) (types.Value, error) {
	var env environment.Environment
	env.Tx = tx
	env.SetRow(r)

	if t.Expr == nil {
		return NullLiteral, errors.New("missing expression")
	}

	return t.Expr.Eval(&env)
}

func (t *ConstraintExpr) Validate(info *database.TableInfo) (err error) {
	Walk(t.Expr, func(e Expr) bool {
		switch e := e.(type) {
		case *Column:
			if info.GetColumnConstraint(e.Name) == nil {
				err = errors.Newf("column %q does not exist", e)
				return false
			}
		}

		return true
	})

	return err
}

func (t *ConstraintExpr) String() string {
	return t.Expr.String()
}
