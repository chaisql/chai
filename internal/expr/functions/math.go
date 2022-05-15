package functions

import (
	"fmt"
	"math"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/types"
)

// MathFunctions returns all math package functions.
func MathFunctions() Definitions {
	return mathFunctions
}

var mathFunctions = Definitions{
	"floor": floor,
	"abs":   abs,
	"acos":  acos,
	"acosh": acosh,
	"asin":  asin,
	"asinh": asinh,
	"atan":  atan,
	"atan2": atan2,
}

var floor = &ScalarDefinition{
	name:  "floor",
	arity: 1,
	callFn: func(args ...types.Value) (types.Value, error) {
		switch args[0].Type() {
		case types.DoubleValue:
			return types.NewDoubleValue(math.Floor(types.As[float64](args[0]))), nil
		case types.IntegerValue:
			return args[0], nil
		default:
			return nil, fmt.Errorf("floor(arg1) expects arg1 to be a number")
		}
	},
}

var abs = &ScalarDefinition{
	name:  "abs",
	arity: 1,
	callFn: func(args ...types.Value) (types.Value, error) {
		if args[0].Type() == types.NullValue {
			return types.NewNullValue(), nil
		}
		v, err := document.CastAs(args[0], types.DoubleValue)
		if err != nil {
			return nil, err
		}
		res := math.Abs(types.As[float64](v))
		if args[0].Type() == types.IntegerValue {
			return document.CastAs(types.NewDoubleValue(res), types.IntegerValue)
		}
		return types.NewDoubleValue(res), nil
	},
}

var acos = &ScalarDefinition{
	name:  "acos",
	arity: 1,
	callFn: func(args ...types.Value) (types.Value, error) {
		if args[0].Type() == types.NullValue {
			return types.NewNullValue(), nil
		}
		v, err := document.CastAs(args[0], types.DoubleValue)
		if err != nil {
			return nil, err
		}
		vv := types.As[float64](v)
		if vv > 1.0 || vv < -1.0 {
			return nil, fmt.Errorf("out of range, acos(arg1) expects arg1 to be within [-1, 1]")
		}
		res := math.Acos(vv)
		return types.NewDoubleValue(res), nil
	},
}

var acosh = &ScalarDefinition{
	name:  "acosh",
	arity: 1,
	callFn: func(args ...types.Value) (types.Value, error) {
		if args[0].Type() == types.NullValue {
			return types.NewNullValue(), nil
		}
		v, err := document.CastAs(args[0], types.DoubleValue)
		if err != nil {
			return nil, err
		}
		vv := types.As[float64](v)
		if vv < 1.0 {
			return nil, fmt.Errorf("out of range, acosh(arg1) expects arg1 >= 1")
		}
		res := math.Acosh(vv)
		return types.NewDoubleValue(res), nil
	},
}

var asin = &ScalarDefinition{
	name:  "asin",
	arity: 1,
	callFn: func(args ...types.Value) (types.Value, error) {
		if args[0].Type() == types.NullValue {
			return types.NewNullValue(), nil
		}
		v, err := document.CastAs(args[0], types.DoubleValue)
		if err != nil {
			return nil, err
		}
		vv := types.As[float64](v)
		if vv > 1.0 || vv < -1.0 {
			return nil, fmt.Errorf("out of range, asin(arg1) expects arg1 to be within [-1, 1]")
		}
		res := math.Asin(vv)
		return types.NewDoubleValue(res), nil
	},
}

var asinh = &ScalarDefinition{
	name:  "asinh",
	arity: 1,
	callFn: func(args ...types.Value) (types.Value, error) {
		v, err := document.CastAs(args[0], types.DoubleValue)
		if err != nil || v.Type() == types.NullValue {
			return v, err
		}
		vv := types.As[float64](v)
		res := math.Asinh(vv)
		return types.NewDoubleValue(res), nil
	},
}

var atan = &ScalarDefinition{
	name:  "atan",
	arity: 1,
	callFn: func(args ...types.Value) (types.Value, error) {
		v, err := document.CastAs(args[0], types.DoubleValue)
		if err != nil || v.Type() == types.NullValue {
			return v, err
		}
		vv := types.As[float64](v)
		res := math.Atan(vv)
		return types.NewDoubleValue(res), nil
	},
}

var atan2 = &ScalarDefinition{
	name:  "atan2",
	arity: 2,
	callFn: func(args ...types.Value) (types.Value, error) {
		vA, err := document.CastAs(args[0], types.DoubleValue)
		if err != nil || vA.Type() == types.NullValue {
			return vA, err
		}
		vvA := types.As[float64](vA)
		vB, err := document.CastAs(args[1], types.DoubleValue)
		if err != nil || vB.Type() == types.NullValue {
			return vB, err
		}
		vvB := types.As[float64](vB)
		res := math.Atan2(vvA, vvB)
		return types.NewDoubleValue(res), nil
	},
}
