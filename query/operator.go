package query

import (
	"bytes"

	"github.com/asdine/genji/field"
)

type cmpOp struct {
	a, b Expr
	tok  Token
}

// Eq creates an expression that returns true if a equals b.
func Eq(a, b Expr) Expr {
	return &cmpOp{a, b, EQ}
}

// Gt creates an expression that returns true if a is greater than b.
func Gt(a, b Expr) Expr {
	return &cmpOp{a, b, GT}
}

// Gte creates an expression that returns true if a is greater than or equal to b.
func Gte(a, b Expr) Expr {
	return &cmpOp{a, b, GTE}
}

// Lt creates an expression that returns true if a is lesser than b.
func Lt(a, b Expr) Expr {
	return &cmpOp{a, b, LT}
}

// Lte creates an expression that returns true if a is lesser than or equal to b.
func Lte(a, b Expr) Expr {
	return &cmpOp{a, b, LTE}
}

func (op cmpOp) Eval(ctx EvalContext) (Scalar, error) {
	sa, err := op.a.Eval(ctx)
	if err != nil {
		return falseScalar, err
	}

	sb, err := op.b.Eval(ctx)
	if err != nil {
		return falseScalar, err
	}

	// if same type, no conversion needed
	if sa.Type == sb.Type || (sa.Type == field.String && sb.Type == field.Bytes) || (sb.Type == field.String && sa.Type == field.Bytes) {
		var ok bool
		switch op.tok {
		case EQ:
			ok = bytes.Equal(sa.Data, sb.Data)
		case GT:
			ok = bytes.Compare(sa.Data, sb.Data) > 0
		case GTE:
			ok = bytes.Compare(sa.Data, sb.Data) >= 0
		case LT:
			ok = bytes.Compare(sa.Data, sb.Data) < 0
		case LTE:
			ok = bytes.Compare(sa.Data, sb.Data) <= 0
		}

		if ok {
			return trueScalar, nil
		}

		return falseScalar, nil
	}

	if len(sa.Data) > 0 && sa.Value == nil {
		sa.Value, err = field.Decode(field.Field{Type: sa.Type, Data: sa.Data})
		if err != nil {
			return falseScalar, err
		}
	}

	if len(sb.Data) > 0 && sb.Value == nil {
		sb.Value, err = field.Decode(field.Field{Type: sb.Type, Data: sb.Data})
		if err != nil {
			return falseScalar, err
		}
	}

	// number OP number
	if field.IsNumber(sa.Type) && field.IsNumber(sb.Type) {
		af, bf := numberToFloat(sa.Value), numberToFloat(sb.Value)

		var ok bool

		switch op.tok {
		case EQ:
			ok = af == bf
		case GT:
			ok = af > bf
		case GTE:
			ok = af >= bf
		case LT:
			ok = af < bf
		case LTE:
			ok = af <= bf
		}

		if ok {
			return trueScalar, nil
		}

		return falseScalar, nil
	}

	return falseScalar, nil
}

func (op cmpOp) Precedence() int {
	return op.tok.Precedence()
}

func (op cmpOp) LeftHand() Expr {
	return op.a
}

func (op cmpOp) RightHand() Expr {
	return op.b
}

func (op *cmpOp) SetLeftHandExpr(a Expr) {
	op.a = a
}

func (op *cmpOp) SetRightHandExpr(b Expr) {
	op.b = b
}

func numberToFloat(v interface{}) float64 {
	var f float64

	switch t := v.(type) {
	case uint:
		f = float64(t)
	case uint8:
		f = float64(t)
	case uint16:
		f = float64(t)
	case uint32:
		f = float64(t)
	case uint64:
		f = float64(t)
	case int:
		f = float64(t)
	case int8:
		f = float64(t)
	case int16:
		f = float64(t)
	case int32:
		f = float64(t)
	case int64:
		f = float64(t)
	}

	return f
}

type andOp struct {
	a, b Expr
}

// And creates an expression that evaluates a and b and returns true if both are truthy.
func And(a, b Expr) Expr {
	return &andOp{a, b}
}

// Eval implements the Expr interface.
func (op *andOp) Eval(ctx EvalContext) (Scalar, error) {
	s, err := op.a.Eval(ctx)
	if err != nil || !s.Truthy() {
		return falseScalar, err
	}

	s, err = op.b.Eval(ctx)
	if err != nil || !s.Truthy() {
		return falseScalar, err
	}

	return trueScalar, nil
}

func (op *andOp) Precedence() int {
	return AND.Precedence()
}

func (op *andOp) LeftHand() Expr {
	return op.a
}

func (op *andOp) RightHand() Expr {
	return op.b
}

func (op *andOp) SetLeftHandExpr(a Expr) {
	op.a = a
}

func (op *andOp) SetRightHandExpr(b Expr) {
	op.b = b
}

type orOp struct {
	a, b Expr
}

// And creates an expression that evaluates a and b and returns true if both are truthy.
func Or(a, b Expr) Expr {
	return &orOp{a, b}
}

// Eval implements the Expr interface.
func (op *orOp) Eval(ctx EvalContext) (Scalar, error) {
	s, err := op.a.Eval(ctx)
	if err != nil {
		return falseScalar, err
	}
	if s.Truthy() {
		return trueScalar, nil
	}

	s, err = op.b.Eval(ctx)
	if err != nil {
		return falseScalar, err
	}
	if s.Truthy() {
		return trueScalar, nil
	}

	return falseScalar, nil
}

func (op *orOp) Precedence() int {
	return OR.Precedence()
}

func (op *orOp) LeftHand() Expr {
	return op.a
}

func (op *orOp) RightHand() Expr {
	return op.b
}

func (op *orOp) SetLeftHandExpr(a Expr) {
	op.a = a
}

func (op *orOp) SetRightHandExpr(b Expr) {
	op.b = b
}
