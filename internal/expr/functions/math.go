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
			return types.NewDoubleValue(math.Floor(args[0].V().(float64))), nil
		case types.IntegerValue:
			return args[0], nil
		default:
			return nil, stringutil.Errorf("floor(arg1) expects arg1 to be a number")
		}
	},
}

var abs = &ScalarDefinition{
	name:  "abs",
	arity: 1,
	callFn: func(args ...document.Value) (document.Value, error) {
		if args[0].Type == document.NullValue {
			return document.NewNullValue(), nil
		}
		v, err := args[0].CastAs(document.DoubleValue)
		if err != nil {
			return document.Value{}, err
		}
		res := math.Abs(v.V.(float64))
		if args[0].Type == document.IntegerValue {
			return document.NewDoubleValue(res).CastAsInteger()
		}
		return document.NewDoubleValue(res), nil
	},
}

var acos = &ScalarDefinition{
	name:  "acos",
	arity: 1,
	callFn: func(args ...document.Value) (document.Value, error) {
		if args[0].Type == document.NullValue {
			return document.NewNullValue(), nil
		}
		v, err := args[0].CastAs(document.DoubleValue)
		if err != nil {
			return document.Value{}, err
		}
		vv := v.V.(float64)
		if vv > 1.0 || vv < -1.0 {
			return document.Value{}, stringutil.Errorf("out of range, acos(arg1) expects arg1 to be within [-1, 1]")
		}
		res := math.Acos(vv)
		return document.NewDoubleValue(res), nil
	},
}

var acosh = &ScalarDefinition{
	name:  "acosh",
	arity: 1,
	callFn: func(args ...document.Value) (document.Value, error) {
		if args[0].Type == document.NullValue {
			return document.NewNullValue(), nil
		}
		v, err := args[0].CastAs(document.DoubleValue)
		if err != nil {
			return document.Value{}, err
		}
		vv := v.V.(float64)
		if vv < 1.0 {
			return document.Value{}, stringutil.Errorf("out of range, acosh(arg1) expects arg1 >= 1")
		}
		res := math.Acosh(vv)
		return document.NewDoubleValue(res), nil
	},
}

var asin = &ScalarDefinition{
	name:  "asin",
	arity: 1,
	callFn: func(args ...document.Value) (document.Value, error) {
		if args[0].Type == document.NullValue {
			return document.NewNullValue(), nil
		}
		v, err := args[0].CastAs(document.DoubleValue)
		if err != nil {
			return document.Value{}, err
		}
		vv := v.V.(float64)
		if vv > 1.0 || vv < -1.0 {
			return document.Value{}, stringutil.Errorf("out of range, asin(arg1) expects arg1 to be within [-1, 1]")
		}
		res := math.Asin(vv)
		return document.NewDoubleValue(res), nil
	},
}

var asinh = &ScalarDefinition{
	name:  "asinh",
	arity: 1,
	callFn: func(args ...document.Value) (document.Value, error) {
		v, err := args[0].CastAs(document.DoubleValue)
		if err != nil || v.Type == document.NullValue {
			return v, err
		}
		vv := v.V.(float64)
		res := math.Asinh(vv)
		return document.NewDoubleValue(res), nil
	},
}

var atan = &ScalarDefinition{
	name:  "atan",
	arity: 1,
	callFn: func(args ...document.Value) (document.Value, error) {
		v, err := args[0].CastAs(document.DoubleValue)
		if err != nil || v.Type == document.NullValue {
			return v, err
		}
		vv := v.V.(float64)
		res := math.Atan(vv)
		return document.NewDoubleValue(res), nil
	},
}

var atan2 = &ScalarDefinition{
	name:  "atan2",
	arity: 2,
	callFn: func(args ...document.Value) (document.Value, error) {
		vA, err := args[0].CastAs(document.DoubleValue)
		if err != nil || vA.Type == document.NullValue {
			return vA, err
		}
		vvA := vA.V.(float64)
		vB, err := args[1].CastAs(document.DoubleValue)
		if err != nil || vB.Type == document.NullValue {
			return vB, err
		}
		vvB := vB.V.(float64)
		res := math.Atan2(vvA, vvB)
		return document.NewDoubleValue(res), nil
	},
}
