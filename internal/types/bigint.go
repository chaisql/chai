package types

import (
	"math"
	"strconv"

	"github.com/chaisql/chai/internal/encoding"
	"github.com/cockroachdb/errors"
)

var _ TypeDefinition = BigintTypeDef{}

type BigintTypeDef struct{}

func (BigintTypeDef) New(v any) Value {
	return NewBigintValue(v.(int64))
}

func (BigintTypeDef) Type() Type {
	return TypeBigint
}

func (BigintTypeDef) Decode(src []byte) (Value, int) {
	x, n := encoding.DecodeInt(src)
	return NewBigintValue(x), n
}

func (BigintTypeDef) IsComparableWith(other Type) bool {
	return other == TypeBigint || other == TypeInteger || other == TypeDouble
}

func (BigintTypeDef) IsIndexComparableWith(other Type) bool {
	return other == TypeBigint || other == TypeInteger
}

var _ Numeric = NewBigintValue(0)
var _ Integral = NewBigintValue(0)
var _ Value = NewBigintValue(0)

type BigintValue int64

// NewBigintValue returns a SQL BIGINT value.
func NewBigintValue(x int64) BigintValue {
	return BigintValue(x)
}

func (v BigintValue) V() any {
	return int64(v)
}

func (v BigintValue) Type() Type {
	return TypeBigint
}

func (v BigintValue) TypeDef() TypeDefinition {
	return BigintTypeDef{}
}

func (v BigintValue) IsZero() (bool, error) {
	return v == 0, nil
}

func (v BigintValue) String() string {
	return strconv.FormatInt(int64(v), 10)
}

func (v BigintValue) MarshalText() ([]byte, error) {
	return []byte(strconv.FormatInt(int64(v), 10)), nil
}

func (v BigintValue) MarshalJSON() ([]byte, error) {
	return v.MarshalText()
}

func (v BigintValue) Encode(dst []byte) ([]byte, error) {
	return encoding.EncodeInt(dst, int64(v)), nil
}

func (v BigintValue) EncodeAsKey(dst []byte) ([]byte, error) {
	return v.Encode(dst)
}

func (v BigintValue) CastAs(target Type) (Value, error) {
	switch target {
	case TypeBigint:
		return v, nil
	case TypeInteger:
		if int64(v) > math.MaxInt32 || int64(v) < math.MinInt32 {
			return nil, errors.Errorf("integer out of range")
		}
		return NewIntegerValue(int32(v)), nil
	case TypeDouble:
		return NewDoubleValue(float64(v)), nil
	case TypeText:
		return NewTextValue(v.String()), nil
	}

	return nil, errors.Errorf("cannot cast %s as %s", v.Type(), target)
}

func (v BigintValue) EQ(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeBigint, TypeInteger:
		return int64(v) == AsInt64(other), nil
	case TypeDouble:
		return float64(int64(v)) == AsFloat64(other), nil
	default:
		return false, nil
	}
}

func (v BigintValue) GT(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeBigint, TypeInteger:
		return int64(v) > AsInt64(other), nil
	case TypeDouble:
		return float64(int64(v)) > AsFloat64(other), nil
	default:
		return false, nil
	}
}

func (v BigintValue) GTE(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeBigint, TypeInteger:
		return int64(v) >= AsInt64(other), nil
	case TypeDouble:
		return float64(int64(v)) >= AsFloat64(other), nil
	default:
		return false, nil
	}
}

func (v BigintValue) LT(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeBigint, TypeInteger:
		return int64(v) < AsInt64(other), nil
	case TypeDouble:
		return float64(int64(v)) <= AsFloat64(other), nil
	default:
		return false, nil
	}
}

func (v BigintValue) LTE(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeBigint, TypeInteger:
		return int64(v) <= AsInt64(other), nil
	case TypeDouble:
		return float64(int64(v)) <= AsFloat64(other), nil
	default:
		return false, nil
	}
}

func (v BigintValue) Between(a, b Value) (bool, error) {
	if !a.Type().IsNumber() || !b.Type().IsNumber() {
		return false, nil
	}

	ok, err := a.LTE(v)
	if err != nil || !ok {
		return false, err
	}

	return b.GTE(v)
}

func (v BigintValue) Add(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeBigint, TypeInteger:
		xa := int64(v)
		xb := AsInt64(other)
		if isAddOverflow(xa, xb, math.MinInt64, math.MaxInt64) {
			return nil, errors.New("bigint out of range")
		}
		xr := xa + xb
		return NewBigintValue(xr), nil
	case TypeDouble:
		return NewDoubleValue(float64(int64(v)) + AsFloat64(other)), nil
	}

	return NewNullValue(), nil
}

func (v BigintValue) Sub(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeBigint, TypeInteger:
		xa := int64(v)
		xb := AsInt64(other)
		if isSubOverflow(xa, xb, math.MinInt64, math.MaxInt64) {
			return nil, errors.New("bigint out of range")
		}
		xr := xa - xb
		return NewBigintValue(xr), nil
	case TypeDouble:
		return NewDoubleValue(float64(int64(v)) - AsFloat64(other)), nil
	}

	return NewNullValue(), nil
}

func (v BigintValue) Mul(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeBigint, TypeInteger:
		xa := int64(v)
		xb := AsInt64(other)
		if xa == 0 || xb == 0 {
			return NewBigintValue(0), nil
		}
		if isMulOverflow(xa, xb, math.MinInt64, math.MaxInt64) {
			return nil, errors.New("bigint out of range")
		}
		xr := xa * xb
		return NewBigintValue(xr), nil
	case TypeDouble:
		return NewDoubleValue(float64(int64(v)) * AsFloat64(other)), nil
	}

	return NewNullValue(), nil
}

func (v BigintValue) Div(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeBigint, TypeInteger:
		xa := int64(v)
		xb := AsInt64(other)
		if xb == 0 {
			return NewNullValue(), nil
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

func (v BigintValue) Mod(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeBigint, TypeInteger:
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

func (v BigintValue) BitwiseAnd(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeBigint, TypeInteger:
		return NewBigintValue(int64(v) & AsInt64(other)), nil
	case TypeDouble:
		xa := int64(v)
		xb := int64(AsFloat64(other))
		return NewBigintValue(xa & xb), nil
	}

	return NewNullValue(), nil
}

func (v BigintValue) BitwiseOr(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeBigint, TypeInteger:
		return NewBigintValue(int64(v) | AsInt64(other)), nil
	case TypeDouble:
		xa := int64(v)
		xb := int64(AsFloat64(other))
		return NewBigintValue(xa | xb), nil
	}

	return NewNullValue(), nil
}

func (v BigintValue) BitwiseXor(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeBigint, TypeInteger:
		return NewBigintValue(int64(v) ^ AsInt64(other)), nil
	case TypeDouble:
		xa := int64(v)
		xb := int64(AsFloat64(other))
		return NewBigintValue(xa ^ xb), nil
	}

	return NewNullValue(), nil
}
