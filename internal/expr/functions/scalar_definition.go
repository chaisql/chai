package functions

import (
	"fmt"
	"strings"

	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/types"
)

// A ScalarDefinition is the definition type for functions which operates on scalar values in contrast to other SQL functions
// such as the SUM aggregator wich operates on expressions instead.
//
// This difference allows to simply define them with a CallFn function that takes multiple document.Value and
// return another types.Value, rather than having to manually evaluate expressions (see Definition).
type ScalarDefinition struct {
	name   string
	arity  int
	callFn func(...types.Value) (types.Value, error)
}

func NewScalarDefinition(name string, arity int, callFn func(...types.Value) (types.Value, error)) *ScalarDefinition {
	return &ScalarDefinition{name: name, arity: arity, callFn: callFn}
}

// Name returns the defined function named (as an ident, so no parentheses).
func (fd *ScalarDefinition) Name() string {
	return fd.name
}

// String returns the defined function name and its arguments.
func (fd *ScalarDefinition) String() string {
	args := make([]string, 0, fd.arity)
	for i := 0; i < fd.arity; i++ {
		args = append(args, fmt.Sprintf("arg%d", i+1))
	}
	return fmt.Sprintf("%s(%s)", fd.name, strings.Join(args, ", "))
}

// Function returns a Function expr node.
func (fd *ScalarDefinition) Function(args ...expr.Expr) (expr.Function, error) {
	if len(args) != fd.arity {
		return nil, fmt.Errorf("%s takes %d argument(s), not %d", fd.String(), fd.arity, len(args))
	}
	return &ScalarFunction{
		params: args,
		def:    fd,
	}, nil
}

// Arity returns the arity of the defined function.
func (fd *ScalarDefinition) Arity() int {
	return fd.arity
}

// A ScalarFunction is a function which operates on scalar values in contrast to other SQL functions
// such as the SUM aggregator wich operates on expressions instead.
type ScalarFunction struct {
	def    *ScalarDefinition
	params []expr.Expr
}

// Eval returns a document.Value based on the given environment and the underlying function
// definition.
func (sf *ScalarFunction) Eval(env *environment.Environment) (types.Value, error) {
	args, err := sf.evalParams(env)
	if err != nil {
		return nil, err
	}
	return sf.def.callFn(args...)
}

// evalParams evaluate all arguments given to the function in the context of the given environmment.
func (sf *ScalarFunction) evalParams(env *environment.Environment) ([]types.Value, error) {
	values := make([]types.Value, 0, len(sf.params))
	for _, param := range sf.params {
		v, err := param.Eval(env)
		if err != nil {
			return nil, err
		}
		values = append(values, v)
	}
	return values, nil
}

// String returns a string represention of the function expression and its arguments.
func (sf *ScalarFunction) String() string {
	return fmt.Sprintf("%s(%v)", sf.def.name, sf.params)
}

// Params return the function arguments.
func (sf *ScalarFunction) Params() []expr.Expr {
	return sf.params
}
