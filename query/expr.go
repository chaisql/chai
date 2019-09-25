package query

import (
	"database/sql/driver"

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
type Expr interface {
	Eval(EvalStack) (Value, error)
}

// EvalStack contains information about the context in which
// the expression is evaluated.
// Any of the members can be nil except the transaction.
type EvalStack struct {
	*EvalStack

	tx     *genji.Tx
	record record.Record
	params []driver.NamedValue
}

func NewStack(from *EvalStack) EvalStack {
	return EvalStack{
		EvalStack: from,
	}
}

func (es EvalStack) Tx() *genji.Tx {
	if es.tx != nil {
		return es.tx
	}

	if es.EvalStack != nil {
		return es.EvalStack.Tx()
	}

	return nil
}

func (es EvalStack) Record() record.Record {
	if es.record != nil {
		return es.record
	}

	if es.EvalStack != nil {
		return es.EvalStack.Record()
	}

	return nil
}

func (es EvalStack) Params() []driver.NamedValue {
	if es.params != nil {
		return es.params
	}

	if es.EvalStack != nil {
		return es.EvalStack.Params()
	}

	return nil
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
func (l LitteralValue) Eval(EvalStack) (Value, error) {
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
func (l LitteralExprList) Eval(ctx EvalStack) (Value, error) {
	if len(l) == 0 {
		return nilLitteral, nil
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
