package query

import (
	"strings"

	"github.com/asdine/genji"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	"github.com/asdine/genji/value"
)

var (
	trueScalar    = Scalar{Type: value.Bool, Data: value.EncodeBool(true)}
	falseScalar   = Scalar{Type: value.Bool, Data: value.EncodeBool(false)}
	trueLitteral  = LitteralValue{Value: value.NewBool(true)}
	falseLitteral = LitteralValue{Value: value.NewBool(true)}
)

// A Scalar represents a value of any type defined by the value package.
type Scalar struct {
	Type  value.Type
	Data  []byte
	Value interface{}
}

// Truthy returns true if the Data is different than the zero value of
// the type of s.
func (s Scalar) Truthy() bool {
	return !value.IsZeroValue(s.Type, s.Data)
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

type EExpr interface {
	Eval(EvalContext) (Value, error)
}

// A Value is the result of evaluating an expression.
type Value interface {
	Truthy() bool
	String() string
}

// A ValueList is a value that contains other values.
type ValueList interface {
	Value

	Iterate(func(Value) error) error
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

// LitteralValueList is a list of values.
type LitteralValueList []Value

// Truthy returns true if the length of l is greater than zero.
// It implements the Value interface.
func (l LitteralValueList) Truthy() bool {
	return len(l) > 0
}

// Eval returns l. It implements the Expr interface.
func (l LitteralValueList) Eval(EvalContext) (Value, error) {
	return l, nil
}

// String returns the string representation of l. It implements the Value interface.
func (l LitteralValueList) String() string {
	var builder strings.Builder

	builder.WriteRune('(')
	for i, v := range l {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(v.String())
	}
	builder.WriteRune(')')
	return builder.String()
}

// FieldValue is a field that can be used as a Value.
type FieldValue field.Field

// String calls the String method of the field.
func (f FieldValue) String() string {
	return field.Field(f).String()
}

// Eval returns l. It implements the Expr interface.
func (f FieldValue) Eval(EvalContext) (Value, error) {
	return f, nil
}

// Truthy returns true if the record is nil
// It implements the Value interface.
func (f FieldValue) Truthy() bool {
	return !value.IsZeroValue(f.Type, f.Data)
}

// RecordValue is a record that can be used as a Value.
type RecordValue struct {
	r record.Record
}

// String returns the string representation of r. It implements the Value interface.
func (r RecordValue) String() string {
	var builder strings.Builder

	builder.WriteRune('(')

	i := 0
	r.r.Iterate(func(f field.Field) error {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(f.String())
		return nil
	})

	builder.WriteRune(')')
	return builder.String()
}

// Eval returns r. It implements the Expr interface.
func (r RecordValue) Eval(EvalContext) (Value, error) {
	return r, nil
}

// Truthy returns true if the record is not nil.
// It implements the Value interface.
func (r RecordValue) Truthy() bool {
	return r.r != nil
}

// Iterate over the list of fields and calls fn for each one sequentially.
func (r RecordValue) Iterate(fn func(Value) error) error {
	return r.r.Iterate(func(f field.Field) error {
		return fn(FieldValue(f))
	})
}

// TableValue is a table.Reader that can be used as a Value.
type TableValue struct {
	r table.Reader
}

// String returns the string representation of t. It implements the Value interface.
func (t TableValue) String() string {
	var builder strings.Builder

	builder.WriteRune('(')

	i := 0
	t.r.Iterate(func(_ []byte, r record.Record) error {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(RecordValue{r}.String())
		return nil
	})

	builder.WriteRune(')')
	return builder.String()
}

// Eval returns t. It implements the Expr interface.
func (t TableValue) Eval(EvalContext) (Value, error) {
	return t, nil
}

// Truthy returns true if the table is not nil.
// It implements the Value interface.
func (t TableValue) Truthy() bool {
	return t.r != nil
}

// Iterate over the list of records and calls fn for each one sequentially.
func (t TableValue) Iterate(fn func(Value) error) error {
	return t.r.Iterate(func(_ []byte, r record.Record) error {
		return fn(RecordValue{r})
	})
}
