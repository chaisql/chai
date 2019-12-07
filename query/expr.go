package query

import (
	"database/sql/driver"
	"fmt"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/scanner"
)

var (
	trueLitteral  = newSingleEvalValue(document.NewBoolValue(true))
	falseLitteral = newSingleEvalValue(document.NewBoolValue(false))
	nilLitteral   = newSingleEvalValue(document.NewNullValue())
)

// An Expr evaluates to a value.
type Expr interface {
	Eval(EvalStack) (EvalValue, error)
}

// EvalStack contains information about the context in which
// the expression is evaluated.
// Any of the members can be nil except the transaction.
type EvalStack struct {
	Tx     *database.Transaction
	Record document.Document
	Params []driver.NamedValue
	Cfg    *database.TableConfig
}

// A EvalValue is the result of evaluating an expression.
type EvalValue struct {
	Value  LiteralValue
	List   LiteralValueList
	IsList bool
}

// Truthy returns true if the Data is different than the zero value of
// the type of s.
// It implements the Value interface.
func (v EvalValue) Truthy() bool {
	if v.IsList {
		return v.List.Truthy()
	}

	return v.Value.Truthy()
}

func newSingleEvalValue(v document.Value) EvalValue {
	return EvalValue{
		Value: LiteralValue{
			Value: v,
		},
	}
}

// A LiteralValue represents a litteral value of any type defined by the value package.
type LiteralValue struct {
	document.Value
}

// BytesValue creates a litteral value of type Bytes.
func BytesValue(v []byte) LiteralValue {
	return LiteralValue{document.NewBytesValue(v)}
}

// StringValue creates a litteral value of type String.
func StringValue(v string) LiteralValue {
	return LiteralValue{document.NewStringValue(v)}
}

// BoolValue creates a litteral value of type Bool.
func BoolValue(v bool) LiteralValue {
	return LiteralValue{document.NewBoolValue(v)}
}

// UintValue creates a litteral value of type Uint.
func UintValue(v uint) LiteralValue {
	return LiteralValue{document.NewUintValue(v)}
}

// Uint8Value creates a litteral value of type Uint8.
func Uint8Value(v uint8) LiteralValue {
	return LiteralValue{document.NewUint8Value(v)}
}

// Uint16Value creates a litteral value of type Uint16.
func Uint16Value(v uint16) LiteralValue {
	return LiteralValue{document.NewUint16Value(v)}
}

// Uint32Value creates a litteral value of type Uint32.
func Uint32Value(v uint32) LiteralValue {
	return LiteralValue{document.NewUint32Value(v)}
}

// Uint64Value creates a litteral value of type Uint64.
func Uint64Value(v uint64) LiteralValue {
	return LiteralValue{document.NewUint64Value(v)}
}

// IntValue creates a litteral value of type Int.
func IntValue(v int) LiteralValue {
	return LiteralValue{document.NewIntValue(v)}
}

// Int8Value creates a litteral value of type Int8.
func Int8Value(v int8) LiteralValue {
	return LiteralValue{document.NewInt8Value(v)}
}

// Int16Value creates a litteral value of type Int16.
func Int16Value(v int16) LiteralValue {
	return LiteralValue{document.NewInt16Value(v)}
}

// Int32Value creates a litteral value of type Int32.
func Int32Value(v int32) LiteralValue {
	return LiteralValue{document.NewInt32Value(v)}
}

// Int64Value creates a litteral value of type Int64.
func Int64Value(v int64) LiteralValue {
	return LiteralValue{document.NewInt64Value(v)}
}

// Float64Value creates a litteral value of type Float64.
func Float64Value(v float64) LiteralValue {
	return LiteralValue{document.NewFloat64Value(v)}
}

// NullValue creates a litteral value of type Null.
func NullValue() LiteralValue {
	return LiteralValue{document.NewNullValue()}
}

// DocumentValue creates a litteral value of type Document.
func DocumentValue(d document.Document) LiteralValue {
	return LiteralValue{document.NewDocumentValue(d)}
}

// Truthy returns true if the Data is different than the zero value of
// the type of s.
// It implements the Value interface.
func (l LiteralValue) Truthy() bool {
	return !l.IsZeroValue()
}

// Eval returns l. It implements the Expr interface.
func (l LiteralValue) Eval(EvalStack) (EvalValue, error) {
	return EvalValue{Value: l}, nil
}

// A LiteralValueList represents a litteral value of any type defined by the value package.
type LiteralValueList []EvalValue

// Truthy returns true if the Data is different than the zero value of
// the type of s.
// It implements the Value interface.
func (l LiteralValueList) Truthy() bool {
	return len(l) > 0
}

// LiteralExprList is a list of expressions.
type LiteralExprList []Expr

// Eval evaluates all the expressions and returns a litteralValueList. It implements the Expr interface.
func (l LiteralExprList) Eval(stack EvalStack) (EvalValue, error) {
	if len(l) == 0 {
		return nilLitteral, nil
	}

	values := make(document.ValueBuffer, len(l))
	for i, e := range l {
		ev, err := e.Eval(stack)
		if err != nil {
			return nilLitteral, err
		}
		values[i] = ev.Value.Value
	}

	var ev EvalValue
	ev.Value.Value = document.NewArrayValue(values)
	return ev, nil
}

type NamedParam string

func (p NamedParam) Eval(stack EvalStack) (EvalValue, error) {
	v, err := p.Extract(stack.Params)
	if err != nil {
		return nilLitteral, err
	}

	vl, err := document.NewValue(v)
	if err != nil {
		return nilLitteral, err
	}

	return newSingleEvalValue(vl), nil
}

func (p NamedParam) Extract(params []driver.NamedValue) (interface{}, error) {
	for _, nv := range params {
		if nv.Name == string(p) {
			return nv.Value, nil
		}
	}

	return nil, fmt.Errorf("param %s not found", p)
}

type PositionalParam int

func (p PositionalParam) Eval(stack EvalStack) (EvalValue, error) {
	v, err := p.Extract(stack.Params)
	if err != nil {
		return nilLitteral, err
	}

	vl, err := document.NewValue(v)
	if err != nil {
		return nilLitteral, err
	}

	return newSingleEvalValue(vl), nil
}

func (p PositionalParam) Extract(params []driver.NamedValue) (interface{}, error) {
	idx := int(p - 1)
	if idx >= len(params) {
		return nil, fmt.Errorf("can't find param number %d", p)
	}

	return params[idx].Value, nil
}

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

// Neq creates an expression that returns true if a equals b.
func Neq(a, b Expr) Expr {
	return CmpOp{SimpleOperator{a, b, scanner.NEQ}}
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

func (op CmpOp) Eval(ctx EvalStack) (EvalValue, error) {
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

func (op CmpOp) compare(l, r EvalValue) (bool, error) {
	switch op.Token {
	case scanner.EQ:
		return l.Value.IsEqual(r.Value.Value)
	case scanner.NEQ:
		return l.Value.IsNotEqual(r.Value.Value)
	case scanner.GT:
		return l.Value.IsGreaterThan(r.Value.Value)
	case scanner.GTE:
		return l.Value.IsGreaterThanOrEqual(r.Value.Value)
	case scanner.LT:
		return l.Value.IsLesserThan(r.Value.Value)
	case scanner.LTE:
		return l.Value.IsLesserThanOrEqual(r.Value.Value)
	default:
		panic(fmt.Sprintf("unknown token %v", op.Token))
	}
}

type AndOp struct {
	SimpleOperator
}

// And creates an expression that evaluates a And b And returns true if both are truthy.
func And(a, b Expr) Expr {
	return &AndOp{SimpleOperator{a, b, scanner.AND}}
}

// Eval implements the Expr interface.
func (op *AndOp) Eval(ctx EvalStack) (EvalValue, error) {
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

// Or creates an expression that first evaluates a, returns true if truthy, then evaluates b, returns true if truthy Or false if falsy.
func Or(a, b Expr) Expr {
	return &OrOp{SimpleOperator{a, b, scanner.OR}}
}

// Eval implements the Expr interface.
func (op *OrOp) Eval(ctx EvalStack) (EvalValue, error) {
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

type KVPair struct {
	K string
	V Expr
}

type KVPairs []KVPair

func (kvp KVPairs) Eval(ctx EvalStack) (EvalValue, error) {
	var fb document.FieldBuffer

	for _, kv := range kvp {
		v, err := kv.V.Eval(ctx)
		if err != nil {
			return EvalValue{}, err
		}

		fb.Add(kv.K, v.Value.Value)
	}

	return EvalValue{
		Value: DocumentValue(&fb),
	}, nil
}
