package expr

import (
	"database/sql/driver"
	"fmt"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/value"
)

var (
	trueLitteral  = NewSingleValue(value.NewBool(true))
	falseLitteral = NewSingleValue(value.NewBool(false))
	NilLitteral   = NewSingleValue(value.NewString("nil"))
)

// An Expr evaluates to a value.
type Expr interface {
	Eval(EvalStack) (Value, error)
}

// EvalStack contains information about the context in which
// the expression is evaluated.
// Any of the members can be nil except the transaction.
type EvalStack struct {
	Tx     *database.Tx
	Record record.Record
	Params []driver.NamedValue
}

// A Value is the result of evaluating an expression.
type Value struct {
	Value  LitteralValue
	List   LitteralValueList
	IsList bool
}

// Truthy returns true if the Data is different than the zero value of
// the type of s.
// It implements the Value interface.
func (v Value) Truthy() bool {
	if v.IsList {
		return v.List.Truthy()
	}

	return v.Value.Truthy()
}

func NewSingleValue(v value.Value) Value {
	return Value{
		Value: LitteralValue{
			Value: v,
		},
	}
}

// A LitteralValue represents a litteral value of any type defined by the value package.
type LitteralValue struct {
	value.Value
}

// Truthy returns true if the Data is different than the zero value of
// the type of s.
// It implements the Value interface.
func (l LitteralValue) Truthy() bool {
	return !value.IsZeroValue(l.Type, l.Data)
}

// Eval returns l. It implements the Expr interface.
func (l LitteralValue) Eval(EvalStack) (Value, error) {
	return Value{Value: l}, nil
}

// A LitteralValueList represents a litteral value of any type defined by the value package.
type LitteralValueList []Value

// Truthy returns true if the Data is different than the zero value of
// the type of s.
// It implements the Value interface.
func (l LitteralValueList) Truthy() bool {
	return len(l) > 0
}

// LitteralExprList is a list of expressions.
type LitteralExprList []Expr

// Eval evaluates all the expressions. If it contains only one element it returns a LitteralValue, otherwise it returns a LitteralValueList. It implements the Expr interface.
func (l LitteralExprList) Eval(stack EvalStack) (Value, error) {
	if len(l) == 0 {
		return NilLitteral, nil
	}

	if len(l) == 1 {
		return l[0].Eval(stack)
	}

	var err error

	values := make(LitteralValueList, len(l))
	for i, e := range l {
		values[i], err = e.Eval(stack)
		if err != nil {
			return NilLitteral, err
		}
	}
	return Value{List: values, IsList: true}, nil
}

type NamedParam string

func (p NamedParam) Eval(stack EvalStack) (Value, error) {
	v, err := p.Extract(stack.Params)
	if err != nil {
		return NilLitteral, err
	}

	vl, err := value.New(v)
	if err != nil {
		return NilLitteral, err
	}

	return NewSingleValue(vl), nil
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

func (p PositionalParam) Eval(stack EvalStack) (Value, error) {
	v, err := p.Extract(stack.Params)
	if err != nil {
		return NilLitteral, err
	}

	vl, err := value.New(v)
	if err != nil {
		return NilLitteral, err
	}

	return NewSingleValue(vl), nil
}

func (p PositionalParam) Extract(params []driver.NamedValue) (interface{}, error) {
	idx := int(p - 1)
	if idx >= len(params) {
		return nil, fmt.Errorf("can't find param number %d", p)
	}

	return params[idx].Value, nil
}
