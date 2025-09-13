package expr

import (
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/types"
)

// A Wildcard is an expression that iterates over all the columns of a row.
type Wildcard struct{}

func (w Wildcard) String() string {
	return "*"
}

func (w Wildcard) Eval(env *environment.Environment) (types.Value, error) {
	panic("not implemented")
}
