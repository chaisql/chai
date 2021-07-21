package functions

import (
	"math"

	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/types"
)

// MathFunctions returns all math package functions.
func MathFunctions() Definitions {
	return mathFunctions
}

var mathFunctions = Definitions{
	"floor": floorFunc,
}

var floorFunc = &ScalarDefinition{
	name:  "floor",
	arity: 1,
	callFn: func(args ...types.Value) (types.Value, error) {
		switch args[0].Type() {
		case types.DoubleValue:
			return types.NewDoubleValue(math.Floor(args[0].V().(float64))), nil
		case types.IntegerValue:
			return args[0], nil
		default:
			return nil, stringutil.Errorf("floor(arg1) expects arg1 to be a number")
		}
	},
}
