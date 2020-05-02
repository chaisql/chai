package expr

import (
	"github.com/asdine/genji/database"
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/sql/scanner"
)

var (
	trueLitteral  = document.NewBoolValue(true)
	falseLitteral = document.NewBoolValue(false)
	nilLitteral   = document.NewNullValue()
)

// An Expr evaluates to a value.
type Expr interface {
	Eval(EvalStack) (document.Value, error)
}

// EvalStack contains information about the context in which
// the expression is evaluated.
// Any of the members can be nil except the transaction.
type EvalStack struct {
	Tx       *database.Transaction
	Document document.Document
	Params   []Param
	Cfg      *database.TableConfig
}

type simpleOperator struct {
	a, b  Expr
	Token scanner.Token
}

func (op simpleOperator) Precedence() int {
	return op.Token.Precedence()
}

func (op simpleOperator) LeftHand() Expr {
	return op.a
}

func (op simpleOperator) RightHand() Expr {
	return op.b
}

func (op *simpleOperator) SetLeftHandExpr(a Expr) {
	op.a = a
}

func (op *simpleOperator) SetRightHandExpr(b Expr) {
	op.b = b
}

func (op *simpleOperator) eval(ctx EvalStack) (document.Value, document.Value, error) {
	va, err := op.a.Eval(ctx)
	if err != nil {
		return nilLitteral, nilLitteral, err
	}

	vb, err := op.b.Eval(ctx)
	if err != nil {
		return nilLitteral, nilLitteral, err
	}

	return va, vb, nil
}
