package functions

import (
	"fmt"
	"strings"

	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/types"
)

var stringsFunction = Definitions {
		"lower": &definition{
		name: "lower",
		arity: 1,
		constructorFn: func(args ...expr.Expr) (expr.Function, error) {
			return &Lower{Expr: args[0]}, nil
		},
	},
}

func StringsDefinitions() Definitions {
	return stringsFunction
}

// Lower is the LOWER function
// It returns the lower-case version of a string
type Lower struct {
	Expr expr.Expr
}

func (s* Lower) Eval(env *environment.Environment) (types.Value, error) {
	val, err := s.Expr.Eval(env)
	if err != nil {
		return nil, err
	}

	if val.Type() != types.TextValue {
		return types.NewNullValue(), nil
	}
	
	lowerCaseString := strings.ToLower(types.As[string](val))

	return types.NewTextValue(lowerCaseString), nil
}

func (s *Lower) IsEqual(other expr.Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(*Lower)
	if !ok {
		return false
	}

	return expr.Equal(s.Expr, o.Expr)
}

func (s *Lower) Params() []expr.Expr { return []expr.Expr{s.Expr} }

func (s *Lower) String() string {
	return fmt.Sprintf("LOWER(%v)", s.Expr)
}