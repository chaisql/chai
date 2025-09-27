package functions

import (
	"fmt"
	"math"
	"math/rand"

	"github.com/chaisql/chai/internal/types"
)

var floor = &ScalarDefinition{
	name:  "floor",
	arity: 1,
	callFn: func(args ...types.Value) (types.Value, error) {
		switch args[0].Type() {
		case types.TypeDoublePrecision:
			return types.NewDoublePrecisionValue(math.Floor(types.AsFloat64(args[0]))), nil
		case types.TypeInteger, types.TypeBigint:
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
		if args[0].Type() == types.TypeNull {
			return types.NewNullValue(), nil
		}
		v, err := args[0].CastAs(types.TypeDoublePrecision)
		if err != nil {
			return nil, err
		}
		res := math.Abs(types.AsFloat64(v))
		if args[0].Type() == types.TypeInteger {
			return types.NewDoublePrecisionValue(res).CastAs(types.TypeInteger)
		}
		if args[0].Type() == types.TypeBigint {
			return types.NewDoublePrecisionValue(res).CastAs(types.TypeBigint)
		}
		return types.NewDoublePrecisionValue(res), nil
	},
}

var acos = &ScalarDefinition{
	name:  "acos",
	arity: 1,
	callFn: func(args ...types.Value) (types.Value, error) {
		if args[0].Type() == types.TypeNull {
			return types.NewNullValue(), nil
		}
		v, err := args[0].CastAs(types.TypeDoublePrecision)
		if err != nil {
			return nil, err
		}
		vv := types.AsFloat64(v)
		if vv > 1.0 || vv < -1.0 {
			return nil, fmt.Errorf("out of range, acos(arg1) expects arg1 to be within [-1, 1]")
		}
		res := math.Acos(vv)
		return types.NewDoublePrecisionValue(res), nil
	},
}

var acosh = &ScalarDefinition{
	name:  "acosh",
	arity: 1,
	callFn: func(args ...types.Value) (types.Value, error) {
		if args[0].Type() == types.TypeNull {
			return types.NewNullValue(), nil
		}
		v, err := args[0].CastAs(types.TypeDoublePrecision)
		if err != nil {
			return nil, err
		}
		vv := types.AsFloat64(v)
		if vv < 1.0 {
			return nil, fmt.Errorf("out of range, acosh(arg1) expects arg1 >= 1")
		}
		res := math.Acosh(vv)
		return types.NewDoublePrecisionValue(res), nil
	},
}

var asin = &ScalarDefinition{
	name:  "asin",
	arity: 1,
	callFn: func(args ...types.Value) (types.Value, error) {
		if args[0].Type() == types.TypeNull {
			return types.NewNullValue(), nil
		}
		v, err := args[0].CastAs(types.TypeDoublePrecision)
		if err != nil {
			return nil, err
		}
		vv := types.AsFloat64(v)
		if vv > 1.0 || vv < -1.0 {
			return nil, fmt.Errorf("out of range, asin(arg1) expects arg1 to be within [-1, 1]")
		}
		res := math.Asin(vv)
		return types.NewDoublePrecisionValue(res), nil
	},
}

var asinh = &ScalarDefinition{
	name:  "asinh",
	arity: 1,
	callFn: func(args ...types.Value) (types.Value, error) {
		v, err := args[0].CastAs(types.TypeDoublePrecision)
		if err != nil || v.Type() == types.TypeNull {
			return v, err
		}
		vv := types.AsFloat64(v)
		res := math.Asinh(vv)
		return types.NewDoublePrecisionValue(res), nil
	},
}

var atan = &ScalarDefinition{
	name:  "atan",
	arity: 1,
	callFn: func(args ...types.Value) (types.Value, error) {
		v, err := args[0].CastAs(types.TypeDoublePrecision)
		if err != nil || v.Type() == types.TypeNull {
			return v, err
		}
		vv := types.AsFloat64(v)
		res := math.Atan(vv)
		return types.NewDoublePrecisionValue(res), nil
	},
}

var atan2 = &ScalarDefinition{
	name:  "atan2",
	arity: 2,
	callFn: func(args ...types.Value) (types.Value, error) {
		vA, err := args[0].CastAs(types.TypeDoublePrecision)
		if err != nil || vA.Type() == types.TypeNull {
			return vA, err
		}
		vvA := types.AsFloat64(vA)
		vB, err := args[1].CastAs(types.TypeDoublePrecision)
		if err != nil || vB.Type() == types.TypeNull {
			return vB, err
		}
		vvB := types.AsFloat64(vB)
		res := math.Atan2(vvA, vvB)
		return types.NewDoublePrecisionValue(res), nil
	},
}

var random = &ScalarDefinition{
	name:  "random",
	arity: 0,
	callFn: func(args ...types.Value) (types.Value, error) {
		randomNum := rand.Int63()
		return types.NewBigintValue(randomNum), nil
	},
}

var sqrt = &ScalarDefinition{
	name:  "sqrt",
	arity: 1,
	callFn: func(args ...types.Value) (types.Value, error) {
		if args[0].Type() != types.TypeDoublePrecision && args[0].Type() != types.TypeInteger {
			return types.NewNullValue(), nil
		}
		v, err := args[0].CastAs(types.TypeDoublePrecision)
		if err != nil {
			return nil, err
		}
		res := math.Sqrt(types.AsFloat64(v))
		return types.NewDoublePrecisionValue(res), nil
	},
}
