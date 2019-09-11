package query

import (
	"github.com/asdine/genji"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/value"
)

var (
	trueLitteral  = LitteralValue{Value: value.NewBool(true)}
	falseLitteral = LitteralValue{Value: value.NewBool(false)}
	nilLitteral   = LitteralValue{Value: value.NewString("nil")}
)

// An Expr evaluates to a value.
// It can be used as an argument to a WHERE clause or any other method that
// expects an expression.
// This package provides several ways of creating expressions.
//
// Using Matchers:
//    And()
//    Or()
//    Eq<T>() (i.e. EqString(), EqInt64(), ...)
//    Gt<T>() (i.e. GtBool(), GtUint(), ...)
//    Gte<T>() (i.e. GteBytes(), GteFloat64(), ...)
//    Lt<T>() (i.e. LtFloat32(), LtUint8(), ...)
//    Lte<T>() (i.e. LteUint16(), LteInt(), ...)
//    ...
//
// Using Values:
//    <T>Value() (i.e. StringValue(), Int32Value(), ...)
type Expr interface {
	Eval(EvalContext) (Value, error)
}

// EvalContext contains information about the context in which
// the expression is evaluated.
// Any of the members can be nil except the transaction.
type EvalContext struct {
	Tx     *genji.Tx
	Record record.Record // can be nil
}

// A Value is the result of evaluating an expression.
type Value interface {
	Truthy() bool
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
func (l LitteralValue) Eval(EvalContext) (Value, error) {
	return l, nil
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
func (l LitteralExprList) Eval(ctx EvalContext) (Value, error) {
	if len(l) == 0 {
		return LitteralValue{}, nil
	}

	if len(l) == 1 {
		return l[0].Eval(ctx)
	}

	var err error

	values := make(LitteralValueList, len(l))
	for i, e := range l {
		values[i], err = e.Eval(ctx)
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}
