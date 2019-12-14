package query

import (
	"database/sql/driver"
	"fmt"

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
	Tx     *database.Transaction
	Record document.Document
	Params []driver.NamedValue
	Cfg    *database.TableConfig
}

// A LiteralValue represents a litteral value of any type defined by the value package.
type LiteralValue document.Value

// BytesValue creates a litteral value of type Bytes.
func BytesValue(v []byte) LiteralValue {
	return LiteralValue(document.NewBytesValue(v))
}

// StringValue creates a litteral value of type String.
func StringValue(v string) LiteralValue {
	return LiteralValue(document.NewStringValue(v))
}

// BoolValue creates a litteral value of type Bool.
func BoolValue(v bool) LiteralValue {
	return LiteralValue(document.NewBoolValue(v))
}

// UintValue creates a litteral value of type Uint.
func UintValue(v uint) LiteralValue {
	return LiteralValue(document.NewUintValue(v))
}

// Uint8Value creates a litteral value of type Uint8.
func Uint8Value(v uint8) LiteralValue {
	return LiteralValue(document.NewUint8Value(v))
}

// Uint16Value creates a litteral value of type Uint16.
func Uint16Value(v uint16) LiteralValue {
	return LiteralValue(document.NewUint16Value(v))
}

// Uint32Value creates a litteral value of type Uint32.
func Uint32Value(v uint32) LiteralValue {
	return LiteralValue(document.NewUint32Value(v))
}

// Uint64Value creates a litteral value of type Uint64.
func Uint64Value(v uint64) LiteralValue {
	return LiteralValue(document.NewUint64Value(v))
}

// IntValue creates a litteral value of type Int.
func IntValue(v int) LiteralValue {
	return LiteralValue(document.NewIntValue(v))
}

// Int8Value creates a litteral value of type Int8.
func Int8Value(v int8) LiteralValue {
	return LiteralValue(document.NewInt8Value(v))
}

// Int16Value creates a litteral value of type Int16.
func Int16Value(v int16) LiteralValue {
	return LiteralValue(document.NewInt16Value(v))
}

// Int32Value creates a litteral value of type Int32.
func Int32Value(v int32) LiteralValue {
	return LiteralValue(document.NewInt32Value(v))
}

// Int64Value creates a litteral value of type Int64.
func Int64Value(v int64) LiteralValue {
	return LiteralValue(document.NewInt64Value(v))
}

// Float64Value creates a litteral value of type Float64.
func Float64Value(v float64) LiteralValue {
	return LiteralValue(document.NewFloat64Value(v))
}

// NullValue creates a litteral value of type Null.
func NullValue() LiteralValue {
	return LiteralValue(document.NewNullValue())
}

// DocumentValue creates a litteral value of type Document.
func DocumentValue(d document.Document) LiteralValue {
	return LiteralValue(document.NewDocumentValue(d))
}

// Eval returns l. It implements the Expr interface.
func (l LiteralValue) Eval(EvalStack) (document.Value, error) {
	return document.Value(l), nil
}

// LiteralExprList is a list of expressions.
type LiteralExprList []Expr

// Eval evaluates all the expressions and returns a litteralValueList. It implements the Expr interface.
func (l LiteralExprList) Eval(stack EvalStack) (document.Value, error) {
	var err error
	values := make(document.ValueBuffer, len(l))
	for i, e := range l {
		values[i], err = e.Eval(stack)
		if err != nil {
			return nilLitteral, err
		}
	}

	return document.NewArrayValue(values), nil
}

type NamedParam string

func (p NamedParam) Eval(stack EvalStack) (document.Value, error) {
	v, err := p.Extract(stack.Params)
	if err != nil {
		return nilLitteral, err
	}

	return document.NewValue(v)
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

func (p PositionalParam) Eval(stack EvalStack) (document.Value, error) {
	v, err := p.Extract(stack.Params)
	if err != nil {
		return nilLitteral, err
	}

	return document.NewValue(v)
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

func (op CmpOp) Eval(ctx EvalStack) (document.Value, error) {
	v1, err := op.a.Eval(ctx)
	if err != nil {
		if err == document.ErrFieldNotFound {
			if op.Token == scanner.NEQ {
				return trueLitteral, nil
			}
			return falseLitteral, nil
		}

		return falseLitteral, err
	}

	v2, err := op.b.Eval(ctx)
	if err != nil {
		if err == document.ErrFieldNotFound {
			if op.Token == scanner.NEQ {
				return trueLitteral, nil
			}

			return falseLitteral, nil
		}

		return falseLitteral, err
	}

	ok, err := op.compare(v1, v2)
	if ok {
		return trueLitteral, err
	}

	return falseLitteral, err
}

func (op CmpOp) compare(l, r document.Value) (bool, error) {
	switch op.Token {
	case scanner.EQ:
		return l.IsEqual(r)
	case scanner.NEQ:
		return l.IsNotEqual(r)
	case scanner.GT:
		return l.IsGreaterThan(r)
	case scanner.GTE:
		return l.IsGreaterThanOrEqual(r)
	case scanner.LT:
		return l.IsLesserThan(r)
	case scanner.LTE:
		return l.IsLesserThanOrEqual(r)
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
func (op *AndOp) Eval(ctx EvalStack) (document.Value, error) {
	s, err := op.a.Eval(ctx)
	if err != nil || !s.IsTruthy() {
		return falseLitteral, err
	}

	s, err = op.b.Eval(ctx)
	if err != nil || !s.IsTruthy() {
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
func (op *OrOp) Eval(ctx EvalStack) (document.Value, error) {
	s, err := op.a.Eval(ctx)
	if err != nil {
		return falseLitteral, err
	}
	if s.IsTruthy() {
		return trueLitteral, nil
	}

	s, err = op.b.Eval(ctx)
	if err != nil {
		return falseLitteral, err
	}
	if s.IsTruthy() {
		return trueLitteral, nil
	}

	return falseLitteral, nil
}

type KVPair struct {
	K string
	V Expr
}

type KVPairs []KVPair

func (kvp KVPairs) Eval(ctx EvalStack) (document.Value, error) {
	var fb document.FieldBuffer

	for _, kv := range kvp {
		v, err := kv.V.Eval(ctx)
		if err != nil {
			return document.Value{}, err
		}

		fb.Add(kv.K, v)
	}

	return document.NewDocumentValue(&fb), nil
}
