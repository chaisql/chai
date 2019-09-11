package query

import (
	"github.com/asdine/genji"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	"github.com/asdine/genji/value"
)

var (
	trueLitteral  = LitteralValue{Value: value.NewBool(true)}
	falseLitteral = LitteralValue{Value: value.NewBool(true)}
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

// FieldExpr is a field that can be used as a Value.
type FieldExpr field.Field

// Eval returns l. It implements the Expr interface.
func (f FieldExpr) Eval(EvalContext) (Value, error) {
	return f, nil
}

// Truthy returns true if the record is nil
// It implements the Value interface.
func (f FieldExpr) Truthy() bool {
	return !value.IsZeroValue(f.Type, f.Data)
}

// // RecordExpr is a record that can be used as a Value.
// type RecordExpr struct {
// 	r record.Record
// }

// // String returns the string representation of r.
// func (r RecordExpr) String() string {
// 	var builder strings.Builder

// 	builder.WriteRune('(')

// 	i := 0
// 	r.r.Iterate(func(f field.Field) error {
// 		if i > 0 {
// 			builder.WriteString(", ")
// 		}
// 		builder.WriteString(f.String())
// 		return nil
// 	})

// 	builder.WriteRune(')')
// 	return builder.String()
// }

// // Eval returns r. It implements the Expr interface.
// func (r RecordExpr) Eval(EvalContext) (v Value, err error) {
// 	err = r.r.Iterate(func(f field.Field) error {
// 		v = FieldExpr(f)
// 		return errStop
// 	})
// 	if err == errStop {
// 		err = nil
// 	}

// 	if v == nil {
// 		v = FieldExpr{}
// 	}

// 	return
// }

// // Truthy returns true if the record is not nil.
// // It implements the Value interface.
// func (r RecordExpr) Truthy() bool {
// 	return r.r != nil
// }

// // Iterate over the list of fields and calls fn for each one sequentially.
// func (r RecordExpr) Iterate(fn func(Expr) error) error {
// 	return r.r.Iterate(func(f field.Field) error {
// 		return fn(FieldExpr(f))
// 	})
// }

// // Length of the record.
// func (r RecordExpr) Length() int {
// 	var i int
// 	r.r.Iterate(func(field.Field) error {
// 		i++
// 		return nil
// 	})
// 	return i
// }

// TableValue is a table.Reader that can be used as an Value.
type TableValue struct {
	r table.Reader
}

// String returns the string representation of t.
// func (t TableValue) String() string {
// 	var builder strings.Builder

// 	builder.WriteRune('(')

// 	i := 0
// 	t.r.Iterate(func(_ []byte, r record.Record) error {
// 		if i > 0 {
// 			builder.WriteString(", ")
// 		}
// 		builder.WriteString(RecordExpr{r}.String())
// 		return nil
// 	})

// 	builder.WriteRune(')')
// 	return builder.String()
// }

// Eval returns t. It implements the Expr interface.
// func (t TableExpr) Eval(EvalContext) (v Value, err error) {
// 	err = t.r.Iterate(func(_ []byte, r record.Record) error {
// 		v = RecordExpr{r}
// 		return errStop
// 	})
// 	if err == errStop {
// 		err = nil
// 	}

// 	if v == nil {
// 		v = RecordExpr{}
// 	}

// 	return
// }

// Truthy returns true if the table is not nil.
// It implements the Value interface.
func (t TableValue) Truthy() bool {
	return t.r != nil
}

// // Iterate over the list of records and calls fn for each one sequentially.
// func (t TableValue) Iterate(fn func(Expr) error) error {
// 	return t.r.Iterate(func(_ []byte, r record.Record) error {
// 		return fn(RecordExpr{r})
// 	})
// }

// // Length of the table.
// func (t TableValue) Length() int {
// 	i, _ := table.NewStream(t.r).Count()
// 	return i
// }

// ValueFromExprList evaluates el recursively until it returns a single value.
// func ValueFromExprList(ctx EvalContext, el ExprList) (Value, error) {
// 	var val Value

// 	err := el.Iterate(func(e Expr) error {
// 		v, err := e.Eval(ctx)
// 		if err != nil {
// 			return err
// 		}

// 		if list, ok := v.(ExprList); ok {
// 			val, err = ValueFromExprList(ctx, list)
// 			if err != nil {
// 				return err
// 			}

// 			return errStop
// 		}

// 		val = v
// 		return errStop
// 	})
// 	if err == errStop {
// 		return val, nil
// 	}
// 	return val, err
// }
