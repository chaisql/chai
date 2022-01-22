package expr

import (
	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/types"
)

// A Path is an expression that extracts a value from a document at a given path.
type Path document.Path

// Eval extracts the current value from the environment and returns the value stored at p.
// It implements the Expr interface.
func (p Path) Eval(env *environment.Environment) (types.Value, error) {
	if len(p) == 0 {
		return NullLiteral, nil
	}

	d, ok := env.GetDocument()
	if !ok {
		return NullLiteral, types.ErrFieldNotFound
	}
	dp := document.Path(p)

	v, ok := env.Get(dp)
	if ok {
		return v, nil
	}

	v, err := dp.GetValueFromDocument(d)
	if errors.Is(err, types.ErrFieldNotFound) {
		return NullLiteral, nil
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

// A Wildcard is an expression that iterates over all the fields of a document.
type Wildcard struct{}

func (w Wildcard) String() string {
	return "*"
}

func (w Wildcard) Eval(env *environment.Environment) (types.Value, error) {
	return nil, errors.New("no table specified")
}

// Iterate call the document iterate method.
func (w Wildcard) Iterate(env environment.Environment, fn func(field string, value types.Value) error) error {
	d, ok := env.GetDocument()
	if !ok {
		return errors.New("no table specified")
	}

	return d.Iterate(fn)
}
