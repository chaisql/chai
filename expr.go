package genji

import (
	"database/sql/driver"
	"fmt"

	"github.com/asdine/genji/internal/scanner"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/value"
)

var (
	trueLitteral  = newSingleEvalValue(value.NewBool(true))
	falseLitteral = newSingleEvalValue(value.NewBool(false))
	nilLitteral   = newSingleEvalValue(value.NewNull())
)

// An expr evaluates to a value.
type expr interface {
	Eval(evalStack) (evalValue, error)
}

// evalStack contains information about the context in which
// the expression is evaluated.
// Any of the members can be nil except the transaction.
type evalStack struct {
	Tx     *Tx
	Record record.Record
	Params []driver.NamedValue
	Cfg    *TableConfig
}

// A evalValue is the result of evaluating an expression.
type evalValue struct {
	Value  litteralValue
	List   litteralValueList
	IsList bool
}

// Truthy returns true if the Data is different than the zero value of
// the type of s.
// It implements the Value interface.
func (v evalValue) Truthy() bool {
	if v.IsList {
		return v.List.Truthy()
	}

	return v.Value.Truthy()
}

func newSingleEvalValue(v value.Value) evalValue {
	return evalValue{
		Value: litteralValue{
			Value: v,
		},
	}
}

// A litteralValue represents a litteral value of any type defined by the value package.
type litteralValue struct {
	value.Value
}

// bytesValue creates a litteral value of type Bytes.
func bytesValue(v []byte) litteralValue {
	return litteralValue{value.NewBytes(v)}
}

// stringValue creates a litteral value of type String.
func stringValue(v string) litteralValue {
	return litteralValue{value.NewString(v)}
}

// boolValue creates a litteral value of type Bool.
func boolValue(v bool) litteralValue {
	return litteralValue{value.NewBool(v)}
}

// uintValue creates a litteral value of type Uint.
func uintValue(v uint) litteralValue {
	return litteralValue{value.NewUint(v)}
}

// uint8Value creates a litteral value of type Uint8.
func uint8Value(v uint8) litteralValue {
	return litteralValue{value.NewUint8(v)}
}

// uint16Value creates a litteral value of type Uint16.
func uint16Value(v uint16) litteralValue {
	return litteralValue{value.NewUint16(v)}
}

// uint32Value creates a litteral value of type Uint32.
func uint32Value(v uint32) litteralValue {
	return litteralValue{value.NewUint32(v)}
}

// uint64Value creates a litteral value of type Uint64.
func uint64Value(v uint64) litteralValue {
	return litteralValue{value.NewUint64(v)}
}

// intValue creates a litteral value of type Int.
func intValue(v int) litteralValue {
	return litteralValue{value.NewInt(v)}
}

// int8Value creates a litteral value of type Int8.
func int8Value(v int8) litteralValue {
	return litteralValue{value.NewInt8(v)}
}

// int16Value creates a litteral value of type Int16.
func int16Value(v int16) litteralValue {
	return litteralValue{value.NewInt16(v)}
}

// int32Value creates a litteral value of type Int32.
func int32Value(v int32) litteralValue {
	return litteralValue{value.NewInt32(v)}
}

// int64Value creates a litteral value of type Int64.
func int64Value(v int64) litteralValue {
	return litteralValue{value.NewInt64(v)}
}

// float64Value creates a litteral value of type Float64.
func float64Value(v float64) litteralValue {
	return litteralValue{value.NewFloat64(v)}
}

// nullValue creates a litteral value of type Null.
func nullValue() litteralValue {
	return litteralValue{value.NewNull()}
}

// Truthy returns true if the Data is different than the zero value of
// the type of s.
// It implements the Value interface.
func (l litteralValue) Truthy() bool {
	return !value.IsZeroValue(l.Type, l.Data)
}

// Eval returns l. It implements the Expr interface.
func (l litteralValue) Eval(evalStack) (evalValue, error) {
	return evalValue{Value: l}, nil
}

// A litteralValueList represents a litteral value of any type defined by the value package.
type litteralValueList []evalValue

// Truthy returns true if the Data is different than the zero value of
// the type of s.
// It implements the Value interface.
func (l litteralValueList) Truthy() bool {
	return len(l) > 0
}

// litteralExprList is a list of expressions.
type litteralExprList []expr

// Eval evaluates all the expressions and returns a litteralValueList. It implements the Expr interface.
func (l litteralExprList) Eval(stack evalStack) (evalValue, error) {
	if len(l) == 0 {
		return nilLitteral, nil
	}

	var err error

	values := make(litteralValueList, len(l))
	for i, e := range l {
		values[i], err = e.Eval(stack)
		if err != nil {
			return nilLitteral, err
		}
	}
	return evalValue{List: values, IsList: true}, nil
}

type namedParam string

func (p namedParam) Eval(stack evalStack) (evalValue, error) {
	v, err := p.Extract(stack.Params)
	if err != nil {
		return nilLitteral, err
	}

	vl, err := value.New(v)
	if err != nil {
		return nilLitteral, err
	}

	return newSingleEvalValue(vl), nil
}

func (p namedParam) Extract(params []driver.NamedValue) (interface{}, error) {
	for _, nv := range params {
		if nv.Name == string(p) {
			return nv.Value, nil
		}
	}

	return nil, fmt.Errorf("param %s not found", p)
}

type positionalParam int

func (p positionalParam) Eval(stack evalStack) (evalValue, error) {
	v, err := p.Extract(stack.Params)
	if err != nil {
		return nilLitteral, err
	}

	vl, err := value.New(v)
	if err != nil {
		return nilLitteral, err
	}

	return newSingleEvalValue(vl), nil
}

func (p positionalParam) Extract(params []driver.NamedValue) (interface{}, error) {
	idx := int(p - 1)
	if idx >= len(params) {
		return nil, fmt.Errorf("can't find param number %d", p)
	}

	return params[idx].Value, nil
}

type simpleOperator struct {
	a, b  expr
	Token scanner.Token
}

func (op simpleOperator) Precedence() int {
	return op.Token.Precedence()
}

func (op simpleOperator) LeftHand() expr {
	return op.a
}

func (op simpleOperator) RightHand() expr {
	return op.b
}

func (op *simpleOperator) SetLeftHandExpr(a expr) {
	op.a = a
}

func (op *simpleOperator) SetRightHandExpr(b expr) {
	op.b = b
}

type cmpOp struct {
	simpleOperator
}

// Eq creates an expression that returns true if a equals b.
func eq(a, b expr) expr {
	return cmpOp{simpleOperator{a, b, scanner.EQ}}
}

// gt creates an expression that returns true if a is greater than b.
func gt(a, b expr) expr {
	return cmpOp{simpleOperator{a, b, scanner.GT}}
}

// gte creates an expression that returns true if a is greater than or equal to b.
func gte(a, b expr) expr {
	return cmpOp{simpleOperator{a, b, scanner.GTE}}
}

// lt creates an expression that returns true if a is lesser than b.
func lt(a, b expr) expr {
	return cmpOp{simpleOperator{a, b, scanner.LT}}
}

// lte creates an expression that returns true if a is lesser than or equal to b.
func lte(a, b expr) expr {
	return cmpOp{simpleOperator{a, b, scanner.LTE}}
}

func (op cmpOp) Eval(ctx evalStack) (evalValue, error) {
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

func (op cmpOp) compare(l, r evalValue) (bool, error) {
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

func (op cmpOp) compareLitterals(l, r litteralValue) (bool, error) {
	switch op.Token {
	case scanner.EQ:
		return l.IsEqual(r.Value)
	case scanner.GT:
		return l.IsGreaterThan(r.Value)
	case scanner.GTE:
		return l.IsGreaterThanOrEqual(r.Value)
	case scanner.LT:
		return l.IsLesserThan(r.Value)
	case scanner.LTE:
		return l.IsLesserThanOrEqual(r.Value)
	default:
		panic(fmt.Sprintf("unknown token %v", op.Token))
	}
}

type andOp struct {
	simpleOperator
}

// and creates an expression that evaluates a and b and returns true if both are truthy.
func and(a, b expr) expr {
	return &andOp{simpleOperator{a, b, scanner.AND}}
}

// Eval implements the Expr interface.
func (op *andOp) Eval(ctx evalStack) (evalValue, error) {
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

// or creates an expression that first evaluates a, returns true if truthy, then evaluates b, returns true if truthy or false if falsy.
func or(a, b expr) expr {
	return &orOp{simpleOperator{a, b, scanner.OR}}
}

// Eval implements the Expr interface.
func (op *orOp) Eval(ctx evalStack) (evalValue, error) {
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
