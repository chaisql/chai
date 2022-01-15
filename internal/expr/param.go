package expr

import (
	"fmt"

	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/types"
)

// NamedParam is an expression which represents the name of a parameter.
type NamedParam string

// Eval looks up for the parameters in the env for the one that has the same name as p
// and returns the value.
func (p NamedParam) Eval(env *environment.Environment) (types.Value, error) {
	return env.GetParamByName(string(p))
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (p NamedParam) IsEqual(other Expr) bool {
	o, ok := other.(NamedParam)
	return ok && p == o
}

// String implements the fmt.Stringer interface.
func (p NamedParam) String() string {
	return fmt.Sprintf("$%s", string(p))
}

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
	return "?"
}
