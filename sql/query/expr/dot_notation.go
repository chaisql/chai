package expr

import (
	"github.com/genjidb/genji/document"
)

// A FieldSelector is a ResultField that extracts a field from a document at a given path.
type FieldSelector document.ValuePath

// Name joins the chunks of the fields selector with the . separator.
func (f FieldSelector) Name() string {
	return f.String()
}

// Eval extracts the document from the context and selects the right field.
// It implements the Expr interface.
func (f FieldSelector) Eval(stack EvalStack) (document.Value, error) {
	if stack.Document == nil {
		return nullLitteral, document.ErrFieldNotFound
	}

	v, err := document.ValuePath(f).GetValue(stack.Document)
	if err == document.ErrFieldNotFound || err == document.ErrValueNotFound {
		return nullLitteral, nil
	}

	return v, nil
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (f FieldSelector) IsEqual(other Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(FieldSelector)
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

func (f FieldSelector) String() string {
	return document.ValuePath(f).String()
}
