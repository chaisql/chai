package expr

import (
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

// A Wildcard is an expression that iterates over all the columns of a row.
type Wildcard struct{}

func (w Wildcard) String() string {
	return "*"
}

func (w Wildcard) Eval(env *environment.Environment) (types.Value, error) {
	panic("not implemented")
}

// Iterate call the object iterate method.
func (w Wildcard) Iterate(env environment.Environment, fn func(field string, value types.Value) error) error {
	r, ok := env.GetRow()
	if !ok {
		return errors.New("no table specified")
	}

	return r.Iterate(fn)
}
