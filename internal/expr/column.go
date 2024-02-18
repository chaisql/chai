package expr

import (
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

type Column struct {
	Name  string
	Table string
}

func (c *Column) String() string {
	return c.Name
}

func (c *Column) IsEqual(other Expr) bool {
	if o, ok := other.(*Column); ok {
		return c.Name == o.Name && c.Table == o.Table
	}

	return false
}

func (c *Column) Eval(env *environment.Environment) (types.Value, error) {
	r, ok := env.GetRow()
	if !ok {
		return NullLiteral, errors.New("no table specified")
	}

	v, err := r.Get(c.Name)
	if err != nil {
		return NullLiteral, err
	}

	return v, nil
}
