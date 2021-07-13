package functions

import (
	"math"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/stringutil"
)

// MathFunctions returns all math package functions.
func MathFunctions() DefinitionsTable {
	return mathFunctions
}

var mathFunctions = DefinitionsTable{
	"floor": floorFunc,
}

var floorFunc = &ScalarFunctionDef{
	name:  "floor",
	arity: 1,
	callFn: func(args ...document.Value) (document.Value, error) {
		switch args[0].Type {
		case document.DoubleValue:
			return document.NewDoubleValue(math.Floor(args[0].V.(float64))), nil
		case document.IntegerValue:
			return args[0], nil
		default:
			return document.Value{}, stringutil.Errorf("floor(arg1) expects arg1 to be a number")
		}
	},
}
