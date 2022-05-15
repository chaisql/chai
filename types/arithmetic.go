package types

import (
	"fmt"
	"math"
)

// Add u to v and return the result.
// Only numeric values and booleans can be added together.
func Add(v1, v2 Value) (res Value, err error) {
	return calculateValues(v1, v2, '+')
}

// Sub calculates v - u and returns the result.
// Only numeric values and booleans can be calculated together.
func Sub(v1, v2 Value) (res Value, err error) {
	return calculateValues(v1, v2, '-')
}

// Mul calculates v * u and returns the result.
// Only numeric values and booleans can be calculated together.
func Mul(v1, v2 Value) (res Value, err error) {
	return calculateValues(v1, v2, '*')
}

// Div calculates v / u and returns the result.
// Only numeric values and booleans can be calculated together.
// If both v and u are integers, the result will be an integer.
func Div(v1, v2 Value) (res Value, err error) {
	return calculateValues(v1, v2, '/')
}

// Mod calculates v / u and returns the result.
// Only numeric values and booleans can be calculated together.
// If both v and u are integers, the result will be an integer.
func Mod(v1, v2 Value) (res Value, err error) {
	return calculateValues(v1, v2, '%')
}

// BitwiseAnd calculates v & u and returns the result.
// Only numeric values and booleans can be calculated together.
// If both v and u are integers, the result will be an integer.
func BitwiseAnd(v1, v2 Value) (res Value, err error) {
	return calculateValues(v1, v2, '&')
}

// BitwiseOr calculates v | u and returns the result.
// Only numeric values and booleans can be calculated together.
// If both v and u are integers, the result will be an integer.
func BitwiseOr(v1, v2 Value) (res Value, err error) {
	return calculateValues(v1, v2, '|')
}

// BitwiseXor calculates v ^ u and returns the result.
// Only numeric values and booleans can be calculated together.
// If both v and u are integers, the result will be an integer.
func BitwiseXor(v1, v2 Value) (res Value, err error) {
	return calculateValues(v1, v2, '^')
}

func calculateValues(a, b Value, operator byte) (res Value, err error) {
	if a.Type() == NullValue || b.Type() == NullValue {
		return NewNullValue(), nil
	}

	if a.Type() == BooleanValue || b.Type() == BooleanValue {
		return NewNullValue(), nil
	}

	if a.Type().IsNumber() && b.Type().IsNumber() {
		if a.Type() == DoubleValue || b.Type() == DoubleValue {
			return calculateFloats(a, b, operator)
		}

		return calculateIntegers(a, b, operator)
	}

	return NewNullValue(), nil
}

func calculateIntegers(a, b Value, operator byte) (res Value, err error) {
	var xa, xb int64

	ia := convertNumberToInteger(a)
	xa = As[int64](ia)

	ib := convertNumberToInteger(b)
	xb = As[int64](ib)

	var xr int64

	switch operator {
	case '-':
		xb = -xb
		fallthrough
	case '+':
		xr = xa + xb
		// if there is an integer overflow
		// convert to float
		if (xr > xa) != (xb > 0) {
			return NewDoubleValue(float64(xa) + float64(xb)), nil
		}
		return NewIntegerValue(xr), nil
	case '*':
		if xa == 0 || xb == 0 {
			return NewIntegerValue(0), nil
		}

		xr = xa * xb
		// if there is no integer overflow
		// return an int, otherwise
		// convert to float
		if (xr < 0) == ((xa < 0) != (xb < 0)) {
			if xr/xb == xa {
				return NewIntegerValue(xr), nil
			}
		}
		return NewDoubleValue(float64(xa) * float64(xb)), nil
	case '/':
		if xb == 0 {
			return NewNullValue(), nil
		}

		return NewIntegerValue(xa / xb), nil
	case '%':
		if xb == 0 {
			return NewNullValue(), nil
		}

		return NewIntegerValue(xa % xb), nil
	case '&':
		return NewIntegerValue(xa & xb), nil
	case '|':
		return NewIntegerValue(xa | xb), nil
	case '^':
		return NewIntegerValue(xa ^ xb), nil
	default:
		panic(fmt.Sprintf("unknown operator %c", operator))
	}
}

func calculateFloats(a, b Value, operator byte) (res Value, err error) {
	var xa, xb float64

	fa := convertNumberToDouble(a)
	xa = As[float64](fa)

	fb := convertNumberToDouble(b)
	xb = As[float64](fb)

	switch operator {
	case '+':
		return NewDoubleValue(xa + xb), nil
	case '-':
		return NewDoubleValue(xa - xb), nil
	case '*':
		return NewDoubleValue(xa * xb), nil
	case '/':
		if xb == 0 {
			return NewNullValue(), nil
		}

		return NewDoubleValue(xa / xb), nil
	case '%':
		mod := math.Mod(xa, xb)

		if math.IsNaN(mod) {
			return NewNullValue(), nil
		}

		return NewDoubleValue(mod), nil
	case '&':
		ia, ib := int64(xa), int64(xb)
		return NewIntegerValue(ia & ib), nil
	case '|':
		ia, ib := int64(xa), int64(xb)
		return NewIntegerValue(ia | ib), nil
	case '^':
		ia, ib := int64(xa), int64(xb)
		return NewIntegerValue(ia ^ ib), nil
	default:
		panic(fmt.Sprintf("unknown operator %c", operator))
	}
}

func convertNumberToInteger(v Value) Value {
	switch v.Type() {
	case IntegerValue:
		return v
	default:
		return NewIntegerValue(int64(As[float64](v)))
	}
}

func convertNumberToDouble(v Value) Value {
	switch v.Type() {
	case DoubleValue:
		return v
	default:
		return NewDoubleValue(float64(As[int64](v)))
	}
}
