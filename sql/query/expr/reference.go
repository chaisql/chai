package expr

import (
	"github.com/genjidb/genji/document"
)

// A Reference is an expression that extracts a value from a document at a given reference.
type Reference document.Reference

// Eval extracts the document from the context and selects the right value.
// It implements the Expr interface.
func (f Reference) Eval(stack EvalStack) (document.Value, error) {
	if stack.Document == nil {
		return nullLitteral, document.ErrFieldNotFound
	}

	v, err := document.Reference(f).GetValue(stack.Document)
	if err == document.ErrFieldNotFound || err == document.ErrValueNotFound {
		return nullLitteral, nil
	}

	return v, nil
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (f Reference) IsEqual(other Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(Reference)
	if !ok {
		return false
	}

	if len(f) != len(o) {
		return false
	}

	for i := range f {
		if f[i] != o[i] {
			return false
		}
	}

	return true
}

func (f Reference) String() string {
	return document.Reference(f).String()
}
