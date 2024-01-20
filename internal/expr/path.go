package expr

import (
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/object"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

// A Path is an expression that extracts a value from a object at a given path.
type Path object.Path

// Eval extracts the current value from the environment and returns the value stored at p.
// It implements the Expr interface.
func (p Path) Eval(env *environment.Environment) (types.Value, error) {
	if len(p) == 0 {
		return NullLiteral, nil
	}

	r, ok := env.GetRow()
	if !ok {
		return NullLiteral, types.ErrFieldNotFound
	}
	dp := object.Path(p)

	v, err := dp.GetValueFromObject(r.Object())
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

	return object.Path(p).IsEqual(object.Path(o))
}

func (p Path) String() string {
	return object.Path(p).String()
}

// A Wildcard is an expression that iterates over all the fields of a object.
type Wildcard struct{}

func (w Wildcard) String() string {
	return "*"
}

func (w Wildcard) Eval(env *environment.Environment) (types.Value, error) {
	r, ok := env.GetRow()
	if !ok {
		return nil, errors.New("no table specified")
	}

	return types.NewObjectValue(r.Object()), nil
}

// Iterate call the object iterate method.
func (w Wildcard) Iterate(env environment.Environment, fn func(field string, value types.Value) error) error {
	r, ok := env.GetRow()
	if !ok {
		return errors.New("no table specified")
	}

	return r.Iterate(fn)
}
