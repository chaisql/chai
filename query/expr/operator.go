package expr

import (
	"bytes"
	"fmt"

	"github.com/asdine/genji/query/scanner"
	"github.com/asdine/genji/value"
)

type SimpleOperator struct {
	a, b  Expr
	Token scanner.Token
}

func (op SimpleOperator) Precedence() int {
	return op.Token.Precedence()
}

func (op SimpleOperator) LeftHand() Expr {
	return op.a
}

func (op SimpleOperator) RightHand() Expr {
	return op.b
}

func (op *SimpleOperator) SetLeftHandExpr(a Expr) {
	op.a = a
}

func (op *SimpleOperator) SetRightHandExpr(b Expr) {
	op.b = b
}

type CmpOp struct {
	SimpleOperator
}

// Eq creates an expression that returns true if a equals b.
func Eq(a, b Expr) Expr {
	return CmpOp{SimpleOperator{a, b, scanner.EQ}}
}

// Gt creates an expression that returns true if a is greater than b.
func Gt(a, b Expr) Expr {
	return CmpOp{SimpleOperator{a, b, scanner.GT}}
}

// Gte creates an expression that returns true if a is greater than or equal to b.
func Gte(a, b Expr) Expr {
	return CmpOp{SimpleOperator{a, b, scanner.GTE}}
}

// Lt creates an expression that returns true if a is lesser than b.
func Lt(a, b Expr) Expr {
	return CmpOp{SimpleOperator{a, b, scanner.LT}}
}

// Lte creates an expression that returns true if a is lesser than or equal to b.
func Lte(a, b Expr) Expr {
	return CmpOp{SimpleOperator{a, b, scanner.LTE}}
}

func (op CmpOp) Eval(ctx EvalStack) (Value, error) {
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

func (op CmpOp) compare(l, r Value) (bool, error) {
	// l must be of the same type
	if !l.IsList {
		if !r.IsList {
			return op.compareLitterals(l.Value, r.Value)
		}
		if len(r.List) == 1 {
			return op.compare(l, r.List[0])
		}

		return false, fmt.Errorf("can't compare expressions")
	}

	if r.IsList {
		// make sure they have the same number of elements
		if len(l.List) != len(r.List) {
			return false, fmt.Errorf("comparing %d elements with %d elements", len(l.List), len(r.List))
		}
		for i := range l.List {
			ok, err := op.compare(l.List[i], r.List[i])
			if err != nil {
				return ok, err
			}
			if !ok {
				return false, nil
			}
		}

		return true, nil
	}
	if len(l.List) == 1 {
		return op.compare(l.List[0], r)
	}

	return false, fmt.Errorf("can't compare expressions")
}

func (op CmpOp) compareLitterals(l, r LitteralValue) (bool, error) {
	var err error

	// if same type, no conversion needed
	if l.Type == r.Type || (l.Type == value.String && r.Type == value.Bytes) || (r.Type == value.String && l.Type == value.Bytes) {
		var ok bool
		switch op.Token {
		case scanner.EQ:
			ok = bytes.Equal(l.Data, r.Data)
		case scanner.GT:
			ok = bytes.Compare(l.Data, r.Data) > 0
		case scanner.GTE:
			ok = bytes.Compare(l.Data, r.Data) >= 0
		case scanner.LT:
			ok = bytes.Compare(l.Data, r.Data) < 0
		case scanner.LTE:
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

		switch op.Token {
		case scanner.EQ:
			ok = af == bf
		case scanner.GT:
			ok = af > bf
		case scanner.GTE:
			ok = af >= bf
		case scanner.LT:
			ok = af < bf
		case scanner.LTE:
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

type AndOp struct {
	SimpleOperator
}

// And creates an expression that evaluates a and b and returns true if both are truthy.
func And(a, b Expr) Expr {
	return &AndOp{SimpleOperator{a, b, scanner.AND}}
}

// Eval implements the Expr interface.
func (op *AndOp) Eval(ctx EvalStack) (Value, error) {
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

type OrOp struct {
	SimpleOperator
}

// Or creates an expression that first evaluates a, returns true if truthy, then evaluates b, returns true if truthy or false if falsy.
func Or(a, b Expr) Expr {
	return &OrOp{SimpleOperator{a, b, scanner.OR}}
}

// Eval implements the Expr interface.
func (op *OrOp) Eval(ctx EvalStack) (Value, error) {
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
