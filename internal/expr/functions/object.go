package functions

import (
	"fmt"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/object"
	"github.com/chaisql/chai/internal/types"
)

var objectsFunctions = Definitions{
	"fields": &definition{
		name:  "fields",
		arity: 1,
		constructorFn: func(args ...expr.Expr) (expr.Function, error) {
			return &ObjectFields{Expr: args[0]}, nil
		},
	},
}

func ObjectsDefinitions() Definitions {
	return objectsFunctions
}

// ObjectFields implements the objects.fields function
// which returns the list of top-level fields of an object.
// If the argument is not an object, it returns null.
type ObjectFields struct {
	Expr expr.Expr
}

func (s *ObjectFields) Eval(env *environment.Environment) (types.Value, error) {
	val, err := s.Expr.Eval(env)
	if err != nil {
		return nil, err
	}

	if val.Type() != types.TypeObject {
		return types.NewNullValue(), nil
	}

	obj := types.As[types.Object](val)
	var fields []string
	err = obj.Iterate(func(k string, _ types.Value) error {
		fields = append(fields, k)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return types.NewArrayValue(object.NewArrayFromSlice(fields)), nil
}

func (s *ObjectFields) IsEqual(other expr.Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(*ObjectFields)
	if !ok {
		return false
	}

	return expr.Equal(s.Expr, o.Expr)
}

func (s *ObjectFields) Params() []expr.Expr { return []expr.Expr{s.Expr} }

func (s *ObjectFields) String() string {
	return fmt.Sprintf("objects.fields(%v)", s.Expr)
}
