package expr

import (
	"time"

	"github.com/asdine/genji/document"
)

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
			return nullLitteral, err
		}
	}

	return document.NewArrayValue(values), nil
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
