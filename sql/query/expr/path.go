package expr

import (
	"errors"

	"github.com/genjidb/genji/document"
)

// A Path is an expression that extracts a value from a document at a given path.
type Path document.Path

// Eval extracts the document from the context and selects the right value.
// It implements the Expr interface.
func (p Path) Eval(env *Environment) (document.Value, error) {
	if len(p) == 0 {
		return nullLitteral, nil
	}

	if _, ok := env.GetCurrentValue(); !ok {
		return nullLitteral, document.ErrFieldNotFound
	}

	v, ok := env.Get(document.Path(p))
	if !ok {
		return nullLitteral, nil
	}

	return v, nil
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

// A Wildcard is an expression that iterates over all the fields of a document.
type Wildcard struct{}

func (w Wildcard) String() string {
	return "*"
}

func (w Wildcard) Eval(env *Environment) (document.Value, error) {
	return document.Value{}, errors.New("no table specified")
}
