package expr

import (
	"strconv"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/types"
)

// PositionalParam is an expression which represents the position of a parameter.
type PositionalParam int

// Eval looks up for the parameters in the env for the one that is has the same position as p
// and returns the value.
func (p PositionalParam) Eval(env *environment.Environment) (types.Value, error) {
	return env.GetParamByIndex(int(p))
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (p PositionalParam) IsEqual(other Expr) bool {
	o, ok := other.(PositionalParam)
	return ok && p == o
}

// String implements the fmt.Stringer interface.
func (p PositionalParam) String() string {
	return "$" + strconv.Itoa(int(p))
}
