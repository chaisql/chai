package query

import (
	"bytes"

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
	zf := field.ZeroValue(s.Type)
	return !bytes.Equal(zf.Data, s.Data)
}

// An Expr evaluates to a scalar.
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
