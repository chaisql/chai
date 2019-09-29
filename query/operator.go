package query

import (
	"bytes"
	"fmt"

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

func (op cmpOp) Eval(ctx EvalStack) (Value, error) {
	v1, err := op.a.Eval(ctx)
	if err != nil {
		return falseLitteral, err
	}

	v2, err := op.b.Eval(ctx)
	if err != nil {
		return falseLitteral, err
	}

	ok, err := op.compare(v1, v2)
	if ok {
		return trueLitteral, err
	}

	return falseLitteral, err
}

func (op cmpOp) compare(l, r Value) (bool, error) {
	// l must be of the same type
	switch t := l.(type) {
	case LitteralValue:
		if v, ok := r.(LitteralValue); ok {
			return op.compareLitterals(t, v)
		}
		if vl, ok := r.(LitteralValueList); ok && len(vl) == 1 {
			return op.compare(t, vl[0])
		}

		return false, fmt.Errorf("can't compare expressions")
	case LitteralValueList:
		if vl, ok := r.(LitteralValueList); ok {
			// make sure they have the same number of elements
			if len(t) != len(vl) {
				return false, fmt.Errorf("comparing %d elements with %d elements", len(t), len(vl))
			}
			for i := range t {
				ok, err := op.compare(t[i], vl[i])
				if err != nil {
					return ok, err
				}
				if !ok {
					return false, nil
				}
			}

			return true, nil
		}
		if v, ok := r.(LitteralValue); ok && len(t) == 1 {
			return op.compare(t[0], v)
		}
	default:
		return false, fmt.Errorf("invalid type %v", l)
	}

	return false, nil
}

func (op cmpOp) compareLitterals(l, r LitteralValue) (bool, error) {
	var err error

	// if same type, no conversion needed
	if l.Type == r.Type || (l.Type == value.String && r.Type == value.Bytes) || (r.Type == value.String && l.Type == value.Bytes) {
		var ok bool
		switch op.tok {
		case EQ:
			ok = bytes.Equal(l.Data, r.Data)
		case GT:
			ok = bytes.Compare(l.Data, r.Data) > 0
		case GTE:
			ok = bytes.Compare(l.Data, r.Data) >= 0
		case LT:
			ok = bytes.Compare(l.Data, r.Data) < 0
		case LTE:
			ok = bytes.Compare(l.Data, r.Data) <= 0
		}

		return ok, nil
	}

	lv, err := l.Decode()
	if err != nil {
		return false, err
	}

	rv, err := r.Decode()
	if err != nil {
		return false, err
	}

	// number OP number
	if value.IsNumber(l.Type) && value.IsNumber(r.Type) {
		af, bf := numberToFloat(lv), numberToFloat(rv)

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
			return true, nil
		}

		return false, nil
	}
	return false, nil
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
func (op *andOp) Eval(ctx EvalStack) (Value, error) {
	s, err := op.a.Eval(ctx)
	if err != nil || !s.Truthy() {
		return falseLitteral, err
	}

	s, err = op.b.Eval(ctx)
	if err != nil || !s.Truthy() {
		return falseLitteral, err
	}

	return trueLitteral, nil
}

type orOp struct {
	simpleOperator
}

// Or creates an expression that first evaluates a, returns true if truthy, then evaluates b, returns true if truthy or false if falsy.
func Or(a, b Expr) Expr {
	return &orOp{simpleOperator{a, b, OR}}
}

// Eval implements the Expr interface.
func (op *orOp) Eval(ctx EvalStack) (Value, error) {
	s, err := op.a.Eval(ctx)
	if err != nil {
		return falseLitteral, err
	}
	if s.Truthy() {
		return trueLitteral, nil
	}

	s, err = op.b.Eval(ctx)
	if err != nil {
		return falseLitteral, err
	}
	if s.Truthy() {
		return trueLitteral, nil
	}

	return falseLitteral, nil
}
