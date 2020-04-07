package query

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/document/encoding"
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

// A LiteralValue represents a litteral value of any type defined by the value package.
type LiteralValue document.Value

// BlobValue creates a litteral value of type Blob.
func BlobValue(v []byte) LiteralValue {
	return LiteralValue(document.NewBlobValue(v))
}

// TextValue creates a litteral value of type Text.
func TextValue(v string) LiteralValue {
	return LiteralValue(document.NewTextValue(v))
}

// BoolValue creates a litteral value of type Bool.
func BoolValue(v bool) LiteralValue {
	return LiteralValue(document.NewBoolValue(v))
}

// IntValue creates a litteral value of type Int.
func IntValue(v int) LiteralValue {
	return LiteralValue(document.NewIntValue(v))
}

// Float64Value creates a litteral value of type Float64.
func Float64Value(v float64) LiteralValue {
	return LiteralValue(document.NewFloat64Value(v))
}

// DurationValue creates a litteral value of type Duration.
func DurationValue(v time.Duration) LiteralValue {
	return LiteralValue(document.NewDurationValue(v))
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

// NamedParam is an expression which represents the name of a parameter.
type NamedParam string

// Eval looks up for the parameters in the stack for the one that has the same name as p
// and returns the value.
func (p NamedParam) Eval(stack EvalStack) (document.Value, error) {
	v, err := p.extract(stack.Params)
	if err != nil {
		return nilLitteral, err
	}

	return document.NewValue(v)
}

func (p NamedParam) extract(params []Param) (interface{}, error) {
	for _, nv := range params {
		if nv.Name == string(p) {
			return nv.Value, nil
		}
	}

	return nil, fmt.Errorf("param %s not found", p)
}

// PositionalParam is an expression which represents the position of a parameter.
type PositionalParam int

// Eval looks up for the parameters in the stack for the one that is has the same position as p
// and returns the value.
func (p PositionalParam) Eval(stack EvalStack) (document.Value, error) {
	v, err := p.extract(stack.Params)
	if err != nil {
		return nilLitteral, err
	}

	return document.NewValue(v)
}

func (p PositionalParam) extract(params []Param) (interface{}, error) {
	idx := int(p - 1)
	if idx >= len(params) {
		return nil, fmt.Errorf("can't find param number %d", p)
	}

	return params[idx].Value, nil
}

// A FieldSelector is a ResultField that extracts a field from a document at a given path.
type FieldSelector []string

// Name joins the chunks of the fields selector with the . separator.
func (f FieldSelector) Name() string {
	return strings.Join(f, ".")
}

// Eval extracts the document from the context and selects the right field.
// It implements the Expr interface.
func (f FieldSelector) Eval(stack EvalStack) (document.Value, error) {
	if stack.Document == nil {
		return nilLitteral, document.ErrFieldNotFound
	}

	var v document.Value
	var a document.Array
	var err error

	for i, chunk := range f {
		if stack.Document != nil {
			v, err = stack.Document.GetByField(chunk)
		} else {
			idx, err := strconv.Atoi(chunk)
			if err != nil {
				return nilLitteral, document.ErrFieldNotFound
			}
			v, err = a.GetByIndex(idx)
		}
		if err != nil {
			return nilLitteral, err
		}

		if i+1 == len(f) {
			break
		}

		stack.Document = nil
		a = nil

		switch v.Type {
		case document.DocumentValue:
			stack.Document, err = v.ConvertToDocument()
		case document.ArrayValue:
			a, err = v.ConvertToArray()
		default:
			return nilLitteral, document.ErrFieldNotFound
		}
		if err != nil {
			return nilLitteral, err
		}
	}

	return v, nil
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

// A CmpOp is a comparison operator.
type CmpOp struct {
	*simpleOperator
}

// NewCmpOp creates a comparison operator.
func NewCmpOp(a, b Expr, t scanner.Token) CmpOp {
	return CmpOp{&simpleOperator{a, b, t}}
}

// Eq creates an expression that returns true if a equals b.
func Eq(a, b Expr) CmpOp {
	return CmpOp{&simpleOperator{a, b, scanner.EQ}}
}

// Neq creates an expression that returns true if a equals b.
func Neq(a, b Expr) CmpOp {
	return CmpOp{&simpleOperator{a, b, scanner.NEQ}}
}

// Gt creates an expression that returns true if a is greater than b.
func Gt(a, b Expr) CmpOp {
	return CmpOp{&simpleOperator{a, b, scanner.GT}}
}

// Gte creates an expression that returns true if a is greater than or equal to b.
func Gte(a, b Expr) CmpOp {
	return CmpOp{&simpleOperator{a, b, scanner.GTE}}
}

// Lt creates an expression that returns true if a is lesser than b.
func Lt(a, b Expr) CmpOp {
	return CmpOp{&simpleOperator{a, b, scanner.LT}}
}

// Lte creates an expression that returns true if a is lesser than or equal to b.
func Lte(a, b Expr) CmpOp {
	return CmpOp{&simpleOperator{a, b, scanner.LTE}}
}

// Eval compares a and b together using the operator specified when constructing the CmpOp
// and returns the result of the comparison.
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

// AndOp is the And operator.
type AndOp struct {
	*simpleOperator
}

// And creates an expression that evaluates a And b And returns true if both are truthy.
func And(a, b Expr) *AndOp {
	return &AndOp{&simpleOperator{a, b, scanner.AND}}
}

// Eval implements the Expr interface. It evaluates a and b and returns true if both evalutate
// to true.
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

// OrOp is the And operator.
type OrOp struct {
	*simpleOperator
}

// Or creates an expression that first evaluates a, returns true if truthy, then evaluates b, returns true if truthy Or false if falsy.
func Or(a, b Expr) Expr {
	return &OrOp{&simpleOperator{a, b, scanner.OR}}
}

// Eval implements the Expr interface. It evaluates a and b and returns true if a or b evalutate
// to true.
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

type addOp struct {
	*simpleOperator
}

// Add creates an expression thats evaluates to the result of a + b.
func Add(a, b Expr) Expr {
	return &addOp{&simpleOperator{a, b, scanner.ADD}}
}

func (op addOp) Eval(ctx EvalStack) (document.Value, error) {
	a, b, err := op.simpleOperator.eval(ctx)
	if err != nil {
		return nilLitteral, err
	}

	return a.Add(b)
}

type subOp struct {
	*simpleOperator
}

// Sub creates an expression thats evaluates to the result of a - b.
func Sub(a, b Expr) Expr {
	return &subOp{&simpleOperator{a, b, scanner.SUB}}
}

func (op subOp) Eval(ctx EvalStack) (document.Value, error) {
	a, b, err := op.simpleOperator.eval(ctx)
	if err != nil {
		return nilLitteral, err
	}

	return a.Sub(b)
}

type mulOp struct {
	*simpleOperator
}

// Mul creates an expression thats evaluates to the result of a * b.
func Mul(a, b Expr) Expr {
	return &mulOp{&simpleOperator{a, b, scanner.MUL}}
}

func (op mulOp) Eval(ctx EvalStack) (document.Value, error) {
	a, b, err := op.simpleOperator.eval(ctx)
	if err != nil {
		return nilLitteral, err
	}

	return a.Mul(b)
}

type divOp struct {
	*simpleOperator
}

// Div creates an expression thats evaluates to the result of a / b.
func Div(a, b Expr) Expr {
	return &divOp{&simpleOperator{a, b, scanner.DIV}}
}

func (op divOp) Eval(ctx EvalStack) (document.Value, error) {
	a, b, err := op.simpleOperator.eval(ctx)
	if err != nil {
		return nilLitteral, err
	}

	return a.Div(b)
}

type modOp struct {
	*simpleOperator
}

// Mod creates an expression thats evaluates to the result of a % b.
func Mod(a, b Expr) Expr {
	return &modOp{&simpleOperator{a, b, scanner.MOD}}
}

func (op modOp) Eval(ctx EvalStack) (document.Value, error) {
	a, b, err := op.simpleOperator.eval(ctx)
	if err != nil {
		return nilLitteral, err
	}

	return a.Mod(b)
}

type bitwiseAndOp struct {
	*simpleOperator
}

// BitwiseAnd creates an expression thats evaluates to the result of a & b.
func BitwiseAnd(a, b Expr) Expr {
	return &bitwiseAndOp{&simpleOperator{a, b, scanner.ADD}}
}

func (op bitwiseAndOp) Eval(ctx EvalStack) (document.Value, error) {
	a, b, err := op.simpleOperator.eval(ctx)
	if err != nil {
		return nilLitteral, err
	}

	return a.BitwiseAnd(b)
}

type bitwiseOrOp struct {
	*simpleOperator
}

// BitwiseOr creates an expression thats evaluates to the result of a & b.
func BitwiseOr(a, b Expr) Expr {
	return &bitwiseOrOp{&simpleOperator{a, b, scanner.ADD}}
}

func (op bitwiseOrOp) Eval(ctx EvalStack) (document.Value, error) {
	a, b, err := op.simpleOperator.eval(ctx)
	if err != nil {
		return nilLitteral, err
	}

	return a.BitwiseOr(b)
}

type bitwiseXorOp struct {
	*simpleOperator
}

// BitwiseXor creates an expression thats evaluates to the result of a & b.
func BitwiseXor(a, b Expr) Expr {
	return &bitwiseXorOp{&simpleOperator{a, b, scanner.ADD}}
}

func (op bitwiseXorOp) Eval(ctx EvalStack) (document.Value, error) {
	a, b, err := op.simpleOperator.eval(ctx)
	if err != nil {
		return nilLitteral, err
	}

	return a.BitwiseXor(b)
}

// KVPair associates an identifier with an expression.
type KVPair struct {
	K string
	V Expr
}

// KVPairs is a list of KVPair.
type KVPairs []KVPair

// Eval turns a list of KVPairs into a document.
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

var functions = map[string]func(args ...Expr) (Expr, error){
	"pk": func(args ...Expr) (Expr, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("pk() takes no arguments")
		}
		return new(PKFunc), nil
	},
}

// GetFunc return a function expression by name.
func GetFunc(name string, args ...Expr) (Expr, error) {
	fn, ok := functions[name]
	if !ok {
		return nil, fmt.Errorf("no such function: %q", name)
	}

	return fn(args...)
}

// PKFunc represents the pk() function.
// It returns the primary key of the current document.
type PKFunc struct{}

// Eval returns the primary key of the current document.
func (k PKFunc) Eval(ctx EvalStack) (document.Value, error) {
	if ctx.Cfg == nil {
		return document.Value{}, errors.New("no table specified")
	}

	pk := ctx.Cfg.GetPrimaryKey()
	if pk != nil {
		return pk.Path.GetValue(ctx.Document)
	}

	return encoding.DecodeValue(document.Int64Value, ctx.Document.(document.Keyer).Key())
}

// Cast represents the CAST expression.
// It returns the primary key of the current document.
type Cast struct {
	Expr      Expr
	ConvertTo document.ValueType
}

// Eval returns the primary key of the current document.
func (c Cast) Eval(ctx EvalStack) (document.Value, error) {
	v, err := c.Expr.Eval(ctx)
	if err != nil {
		return v, err
	}

	return v.ConvertTo(c.ConvertTo)
}
