package query

import (
	"bytes"

	"github.com/asdine/genji/value"
)

type simpleOperator struct {
	a, b Expr
	tok  Token
}

func (op simpleOperator) Precedence() int {
	return op.tok.Precedence()
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

type cmpOp struct {
	simpleOperator
}

// Eq creates an expression that returns true if a equals b.
func Eq(a, b Expr) Expr {
	return cmpOp{simpleOperator{a, b, EQ}}
}

// Gt creates an expression that returns true if a is greater than b.
func Gt(a, b Expr) Expr {
	return cmpOp{simpleOperator{a, b, GT}}
}

// Gte creates an expression that returns true if a is greater than or equal to b.
func Gte(a, b Expr) Expr {
	return cmpOp{simpleOperator{a, b, GTE}}
}

// Lt creates an expression that returns true if a is lesser than b.
func Lt(a, b Expr) Expr {
	return cmpOp{simpleOperator{a, b, LT}}
}

// Lte creates an expression that returns true if a is lesser than or equal to b.
func Lte(a, b Expr) Expr {
	return cmpOp{simpleOperator{a, b, LTE}}
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
	if sa.Type == sb.Type || (sa.Type == value.String && sb.Type == value.Bytes) || (sb.Type == value.String && sa.Type == value.Bytes) {
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
		sa.Value, err = value.Value{Type: sa.Type, Data: sa.Data}.Decode()
		if err != nil {
			return falseScalar, err
		}
	}

	if len(sb.Data) > 0 && sb.Value == nil {
		sb.Value, err = value.Value{Type: sb.Type, Data: sb.Data}.Decode()
		if err != nil {
			return falseScalar, err
		}
	}

	// number OP number
	if value.IsNumber(sa.Type) && value.IsNumber(sb.Type) {
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
	simpleOperator
}

// And creates an expression that evaluates a and b and returns true if both are truthy.
func And(a, b Expr) Expr {
	return &andOp{simpleOperator{a, b, AND}}
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

type orOp struct {
	simpleOperator
}

// Or creates an expression that first evaluates a, returns true if truthy, then evaluates b, returns true if truthy or false if falsy.
func Or(a, b Expr) Expr {
	return &orOp{simpleOperator{a, b, OR}}
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
