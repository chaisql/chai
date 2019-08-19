package query

import (
	"github.com/asdine/genji"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
)

var (
	trueScalar  = Scalar{Type: field.Bool, Data: field.EncodeBool(true)}
	falseScalar = Scalar{Type: field.Bool, Data: field.EncodeBool(false)}
)

// A Scalar represents a value of any type defined by the field package.
type Scalar struct {
	Type field.Type
	Data []byte
}

// Truthy returns true if the Data is different than the zero value of
// the type of s.
func (s Scalar) Truthy() bool {
	return !field.IsZeroValue(s.Type, s.Data)
}

// Eval returns s. It implements the Expr interface.
func (s Scalar) Eval(EvalContext) (Scalar, error) {
	return s, nil
}

// An Expr evaluates to a scalar.
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
	Eval(EvalContext) (Scalar, error)
}

// EvalContext contains information about the context in which
// the expression is evaluated.
// Any of the members can be nil except the transaction.
type EvalContext struct {
	Tx     *genji.Tx
	Record record.Record // can be nil
}
