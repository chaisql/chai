package types

import (
	"math"
	"strconv"
)

var _ Numeric = NewIntegerValue(0)

type IntegerValue int64

// NewIntegerValue returns a SQL INTEGER value.
func NewIntegerValue(x int64) IntegerValue {
	return IntegerValue(x)
}

func (v IntegerValue) V() any {
	return int64(v)
}

func (v IntegerValue) Type() ValueType {
	return TypeInteger
}

func (v IntegerValue) IsZero() (bool, error) {
	return v == 0, nil
}

func (v IntegerValue) String() string {
	return strconv.FormatInt(int64(v), 10)
}

func (v IntegerValue) MarshalText() ([]byte, error) {
	return []byte(strconv.FormatInt(int64(v), 10)), nil
}

func (v IntegerValue) MarshalJSON() ([]byte, error) {
	return v.MarshalText()
}

func (v IntegerValue) EQ(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeInteger:
		return int64(v) == AsInt64(other), nil
	case TypeDouble:
		return float64(int64(v)) == AsFloat64(other), nil
	default:
		return false, nil
	}
}

func (v IntegerValue) GT(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeInteger:
		return int64(v) > AsInt64(other), nil
	case TypeDouble:
		return float64(int64(v)) > AsFloat64(other), nil
	default:
		return false, nil
	}
}

func (v IntegerValue) GTE(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeInteger:
		return int64(v) >= AsInt64(other), nil
	case TypeDouble:
		return float64(int64(v)) >= AsFloat64(other), nil
	default:
		return false, nil
	}
}

func (v IntegerValue) LT(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeInteger:
		return int64(v) < AsInt64(other), nil
	case TypeDouble:
		return float64(int64(v)) <= AsFloat64(other), nil
	default:
		return false, nil
	}
}

func (v IntegerValue) LTE(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeInteger:
		return int64(v) <= AsInt64(other), nil
	case TypeDouble:
		return float64(int64(v)) <= AsFloat64(other), nil
	default:
		return false, nil
	}
}

func (v IntegerValue) Between(a, b Value) (bool, error) {
	if !a.Type().IsNumber() || !b.Type().IsNumber() {
		return false, nil
	}

	ok, err := a.LTE(v)
	if err != nil || !ok {
		return false, err
	}

	return b.GTE(v)
}

func (v IntegerValue) Add(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeInteger:
		xa := int64(v)
		xb := AsInt64(other)
		xr := xa + xb
		// if there is an integer overflow
		// convert to float
		if (xr > xa) != (xb > 0) {
			return NewDoubleValue(float64(xa) + float64(xb)), nil
		}
		return NewIntegerValue(xr), nil
	case TypeDouble:
		return NewDoubleValue(float64(int64(v)) + AsFloat64(other)), nil
	}

	return NewNullValue(), nil
}

func (v IntegerValue) Sub(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeInteger:
		xa := int64(v)
		xb := AsInt64(other)
		xr := xa - xb
		// if there is an integer overflow
		// convert to float
		if (xr < xa) != (xb > 0) {
			return NewDoubleValue(float64(xa) - float64(xb)), nil
		}
		return NewIntegerValue(xr), nil
	case TypeDouble:
		return NewDoubleValue(float64(int64(v)) - AsFloat64(other)), nil
	}

	return NewNullValue(), nil
}

func (v IntegerValue) Mul(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeInteger:
		xa := int64(v)
		xb := AsInt64(other)
		if xa == 0 || xb == 0 {
			return NewIntegerValue(0), nil
		}
		xr := xa * xb
		// if there is no integer overflow
		// return an int, otherwise
		// convert to float
		if (xr < 0) == ((xa < 0) != (xb < 0)) {
			if xr/xb == xa {
				return NewIntegerValue(xr), nil
			}
		}

		return NewDoubleValue(float64(xa) * float64(xb)), nil
	case TypeDouble:
		return NewDoubleValue(float64(int64(v)) * AsFloat64(other)), nil
	}

	return NewNullValue(), nil
}

func (v IntegerValue) Div(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeInteger:
		xa := int64(v)
		xb := AsInt64(other)
		if xb == 0 {
			return NewNullValue(), nil
		}

		return NewIntegerValue(xa / xb), nil
	case TypeDouble:
		xa := float64(AsInt64(v))
		xb := AsFloat64(other)
		if xb == 0 {
			return NewNullValue(), nil
		}

		return NewDoubleValue(xa / xb), nil
	}

	return NewNullValue(), nil
}

func (v IntegerValue) Mod(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeInteger:
		xa := int64(v)
		xb := AsInt64(other)
		if xb == 0 {
			return NewNullValue(), nil
		}

		return NewIntegerValue(xa % xb), nil
	case TypeDouble:
		xa := float64(AsInt64(v))
		xb := AsFloat64(other)
		mod := math.Mod(xa, xb)
		if math.IsNaN(mod) {
			return NewNullValue(), nil
		}

		return NewDoubleValue(mod), nil
	}

	return NewNullValue(), nil
}

func (v IntegerValue) BitwiseAnd(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeInteger:
		return NewIntegerValue(int64(v) & AsInt64(other)), nil
	case TypeDouble:
		xa := int64(v)
		xb := int64(AsFloat64(other))
		return NewIntegerValue(xa & xb), nil
	}

	return NewNullValue(), nil
}

func (v IntegerValue) BitwiseOr(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeInteger:
		return NewIntegerValue(int64(v) | AsInt64(other)), nil
	case TypeDouble:
		xa := int64(v)
		xb := int64(AsFloat64(other))
		return NewIntegerValue(xa | xb), nil
	}

	return NewNullValue(), nil
}

func (v IntegerValue) BitwiseXor(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeInteger:
		return NewIntegerValue(int64(v) ^ AsInt64(other)), nil
	case TypeDouble:
		xa := int64(v)
		xb := int64(AsFloat64(other))
		return NewIntegerValue(xa ^ xb), nil
	}

	return NewNullValue(), nil
}
