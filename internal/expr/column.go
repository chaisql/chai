package expr

import (
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

type Column string

func (c Column) String() string {
	return string(c)
}

func (c Column) Eval(env *environment.Environment) (types.Value, error) {
	r, ok := env.GetRow()
	if !ok {
		return NullLiteral, errors.New("no table specified")
	}

	v, err := r.Get(string(c))
	if err != nil {
		return NullLiteral, err
	}

	return v, nil
}
