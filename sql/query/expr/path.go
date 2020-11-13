package expr

import (
	"github.com/genjidb/genji/document"
)

// A Path is an expression that extracts a value from a document at a given path.
type Path document.Path

// Eval extracts the document from the context and selects the right value.
// It implements the Expr interface.
func (p Path) Eval(stack EvalStack) (document.Value, error) {
	if stack.Document == nil {
		return nullLitteral, document.ErrFieldNotFound
	}

	v, err := document.Path(p).GetValue(stack.Document)
	if err == document.ErrFieldNotFound || err == document.ErrValueNotFound {
		return nullLitteral, nil
	}

	return v, err
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (p Path) IsEqual(other Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(Path)
	if !ok {
		return false
	}

	return document.Path(p).IsEqual(document.Path(o))
}

func (p Path) String() string {
	return document.Path(p).String()
}
