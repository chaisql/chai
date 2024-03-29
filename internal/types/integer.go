package types

import (
	"math"
	"strconv"

	"github.com/chaisql/chai/internal/encoding"
	"github.com/cockroachdb/errors"
)

var _ TypeDefinition = IntegerTypeDef{}

type IntegerTypeDef struct{}

func (IntegerTypeDef) New(v any) Value {
	return NewIntegerValue(v.(int32))
}

func (IntegerTypeDef) Type() Type {
	return TypeInteger
}

func (IntegerTypeDef) Decode(src []byte) (Value, int) {
	x, n := encoding.DecodeInt(src)
	if x < math.MinInt32 || x > math.MaxInt32 {
		panic(errors.New("integer out of range"))
	}

	return NewIntegerValue(int32(x)), n
}

func (IntegerTypeDef) IsComparableWith(other Type) bool {
	return other == TypeInteger || other == TypeBigint || other == TypeDouble
}

func (IntegerTypeDef) IsIndexComparableWith(other Type) bool {
	return other == TypeInteger || other == TypeBigint
}

var _ Numeric = NewIntegerValue(0)
var _ Integral = NewIntegerValue(0)
var _ Value = NewIntegerValue(0)

type IntegerValue int32

// NewIntegerValue returns a SQL INTEGER value.
func NewIntegerValue(x int32) IntegerValue {
	return IntegerValue(x)
}

func (v IntegerValue) V() any {
	return int32(v)
}

func (v IntegerValue) Type() Type {
	return TypeInteger
}

func (v IntegerValue) TypeDef() TypeDefinition {
	return IntegerTypeDef{}
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

func (v IntegerValue) Encode(dst []byte) ([]byte, error) {
	return encoding.EncodeInt(dst, int64(v)), nil
}

func (v IntegerValue) EncodeAsKey(dst []byte) ([]byte, error) {
	return v.Encode(dst)
}

func (v IntegerValue) CastAs(target Type) (Value, error) {
	switch target {
	case TypeInteger:
		return v, nil
	case TypeBoolean:
		return NewBooleanValue(int32(v) != 0), nil
	case TypeBigint:
		return NewBigintValue(int64(v)), nil
	case TypeDouble:
		return NewDoubleValue(float64(v)), nil
	case TypeText:
		return NewTextValue(v.String()), nil
	}

	return nil, errors.Errorf("cannot cast %s as %s", v.Type(), target)
}

func (v IntegerValue) EQ(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeInteger:
		return int32(v) == AsInt32(other), nil
	case TypeBigint:
		return int64(v) == AsInt64(other), nil
	case TypeDouble:
		return float64(int32(v)) == AsFloat64(other), nil
	default:
		return false, nil
	}
}

func (v IntegerValue) GT(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeInteger:
		return int32(v) > AsInt32(other), nil
	case TypeBigint:
		return int64(v) > AsInt64(other), nil
	case TypeDouble:
		return float64(int32(v)) > AsFloat64(other), nil
	default:
		return false, nil
	}
}

func (v IntegerValue) GTE(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeInteger:
		return int32(v) >= AsInt32(other), nil
	case TypeBigint:
		return int64(v) >= AsInt64(other), nil
	case TypeDouble:
		return float64(int32(v)) >= AsFloat64(other), nil
	default:
		return false, nil
	}
}

func (v IntegerValue) LT(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeInteger:
		return int32(v) < AsInt32(other), nil
	case TypeBigint:
		return int64(v) < AsInt64(other), nil
	case TypeDouble:
		return float64(int32(v)) <= AsFloat64(other), nil
	default:
		return false, nil
	}
}

func (v IntegerValue) LTE(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeInteger:
		return int32(v) <= AsInt32(other), nil
	case TypeBigint:
		return int64(v) <= AsInt64(other), nil
	case TypeDouble:
		return float64(int32(v)) <= AsFloat64(other), nil
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
		xa := int32(v)
		xb := AsInt32(other)
		if isAddOverflow(xa, xb, math.MinInt32, math.MaxInt32) {
			return nil, errors.New("integer out of range")
		}

		xr := xa + xb
		return NewIntegerValue(xr), nil
	case TypeBigint:
		xa := int64(v)
		xb := AsInt64(other)
		if isAddOverflow(xa, xb, math.MinInt64, math.MaxInt64) {
			return nil, errors.New("bigint out of range")
		}

		xr := xa + xb
		return NewBigintValue(xr), nil
	case TypeDouble:
		return NewDoubleValue(float64(int32(v)) + AsFloat64(other)), nil
	}

	return NewNullValue(), nil
}

func (v IntegerValue) Sub(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeInteger:
		xa := int32(v)
		xb := AsInt32(other)
		if isSubOverflow(xa, xb, math.MinInt32, math.MaxInt32) {
			return nil, errors.New("integer out of range")
		}

		xr := xa - xb
		return NewIntegerValue(xr), nil
	case TypeBigint:
		xa := int64(v)
		xb := AsInt64(other)
		if isSubOverflow(xa, xb, math.MinInt64, math.MaxInt64) {
			return nil, errors.New("bigint out of range")
		}
		xr := xa - xb
		return NewBigintValue(xr), nil
	case TypeDouble:
		return NewDoubleValue(float64(int32(v)) - AsFloat64(other)), nil
	}

	return NewNullValue(), nil
}

func (v IntegerValue) Mul(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeInteger:
		xa := int32(v)
		xb := AsInt32(other)
		if isMulOverflow(xa, xb, math.MinInt32, math.MaxInt32) {
			return nil, errors.New("integer out of range")
		}
		xr := xa * xb

		return NewIntegerValue(xr), nil
	case TypeBigint:
		xa := int64(v)
		xb := AsInt64(other)
		if isMulOverflow(xa, xb, math.MinInt64, math.MaxInt64) {
			return nil, errors.New("integer out of range")
		}

		xr := xa * xb
		return NewBigintValue(xr), nil
	case TypeDouble:
		return NewDoubleValue(float64(int32(v)) * AsFloat64(other)), nil
	}

	return NewNullValue(), nil
}

func (v IntegerValue) Div(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeInteger:
		xa := int32(v)
		xb := AsInt32(other)
		if xb == 0 {
			return nil, errors.New("division by zero")
		}

		return NewIntegerValue(xa / xb), nil
	case TypeBigint:
		xa := int64(v)
		xb := AsInt64(other)
		if xb == 0 {
			return nil, errors.New("division by zero")
		}

		return NewBigintValue(xa / xb), nil
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
		xa := int32(v)
		xb := AsInt32(other)
		if xb == 0 {
			return NewNullValue(), nil
		}

		return NewIntegerValue(xa % xb), nil
	case TypeBigint:
		xa := int64(v)
		xb := AsInt64(other)
		if xb == 0 {
			return NewNullValue(), nil
		}

		return NewBigintValue(xa % xb), nil
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
		return NewIntegerValue(int32(v) & AsInt32(other)), nil
	case TypeBigint:
		return NewBigintValue(int64(v) & AsInt64(other)), nil
	case TypeDouble:
		xa := int32(v)
		xb := int32(AsFloat64(other))
		return NewIntegerValue(xa & xb), nil
	}

	return NewNullValue(), nil
}

func (v IntegerValue) BitwiseOr(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeInteger:
		return NewIntegerValue(int32(v) | AsInt32(other)), nil
	case TypeBigint:
		return NewBigintValue(int64(v) | AsInt64(other)), nil
	case TypeDouble:
		xa := int32(v)
		xb := int32(AsFloat64(other))
		return NewIntegerValue(xa | xb), nil
	}

	return NewNullValue(), nil
}

func (v IntegerValue) BitwiseXor(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeInteger:
		return NewIntegerValue(int32(v) ^ AsInt32(other)), nil
	case TypeBigint:
		return NewBigintValue(int64(v) ^ AsInt64(other)), nil
	case TypeDouble:
		xa := int32(v)
		xb := int32(AsFloat64(other))
		return NewIntegerValue(xa ^ xb), nil
	}

	return NewNullValue(), nil
}
