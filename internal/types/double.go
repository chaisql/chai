package types

import (
	"math"
	"strconv"

	"github.com/chaisql/chai/internal/encoding"
	"github.com/cockroachdb/errors"
)

var _ TypeDefinition = DoublePrecisionTypeDef{}

type DoublePrecisionTypeDef struct{}

func (DoublePrecisionTypeDef) Decode(src []byte) (Value, int) {
	x, n := encoding.DecodeFloat(src)
	return NewDoublePrevisionValue(x), n
}

func (DoublePrecisionTypeDef) IsComparableWith(other Type) bool {
	return other == TypeDoublePrecision || other == TypeInteger || other == TypeBigint
}

func (DoublePrecisionTypeDef) IsIndexComparableWith(other Type) bool {
	return other == TypeDoublePrecision
}

var _ Numeric = NewDoublePrevisionValue(0)

type DoublePrecisionValue float64

// NewDoublePrevisionValue returns a SQL DOUBLE PRECISION value.
func NewDoublePrevisionValue(x float64) DoublePrecisionValue {
	return DoublePrecisionValue(x)
}

func (v DoublePrecisionValue) V() any {
	return float64(v)
}

func (v DoublePrecisionValue) Type() Type {
	return TypeDoublePrecision
}

func (v DoublePrecisionValue) TypeDef() TypeDefinition {
	return DoublePrecisionTypeDef{}
}

func (v DoublePrecisionValue) IsZero() (bool, error) {
	return v == 0, nil
}

func (v DoublePrecisionValue) String() string {
	f := AsFloat64(v)
	abs := math.Abs(f)
	fmt := byte('f')
	if abs != 0 {
		if abs < 1e-6 || abs >= 1e15 {
			fmt = 'e'
		}
	}

	// By default the precision is -1 to use the smallest number of digits.
	// See https://pkg.go.dev/strconv#FormatFloat
	prec := -1
	// if the number is round, add .0
	if float64(int64(f)) == f {
		prec = 1
	}
	return strconv.FormatFloat(AsFloat64(v), fmt, prec, 64)
}

func (v DoublePrecisionValue) MarshalText() ([]byte, error) {
	return []byte(v.String()), nil
}

func (v DoublePrecisionValue) MarshalJSON() ([]byte, error) {
	f := AsFloat64(v)
	abs := math.Abs(f)
	fmt := byte('f')
	if abs != 0 {
		if abs < 1e-6 || abs >= 1e15 {
			fmt = 'e'
		}
	}

	// By default the precision is -1 to use the smallest number of digits.
	// See https://pkg.go.dev/strconv#FormatFloat
	prec := -1
	return strconv.AppendFloat(nil, AsFloat64(v), fmt, prec, 64), nil
}

func (v DoublePrecisionValue) Encode(dst []byte) ([]byte, error) {
	return encoding.EncodeFloat(dst, float64(v)), nil
}

func (v DoublePrecisionValue) EncodeAsKey(dst []byte) ([]byte, error) {
	return encoding.EncodeFloat64(dst, float64(v)), nil
}

func (v DoublePrecisionValue) CastAs(target Type) (Value, error) {
	switch target {
	case TypeDoublePrecision:
		return v, nil
	case TypeInteger:
		f := float64(v)
		if f > 0 && (int32(f) < 0 || f >= math.MaxInt32) {
			return nil, errors.New("integer out of range")
		}
		return NewIntegerValue(int32(v)), nil
	case TypeBigint:
		f := float64(v)
		if f > 0 && (int64(f) < 0 || f >= math.MaxInt64) {
			return nil, errors.New("integer out of range")
		}
		return NewBigintValue(int64(v)), nil
	case TypeText:
		enc, err := v.MarshalJSON()
		if err != nil {
			return nil, err
		}
		return NewTextValue(string(enc)), nil
	}

	return nil, errors.Errorf("cannot cast %s as %s", v.Type(), target)
}

func (v DoublePrecisionValue) EQ(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeDoublePrecision:
		return float64(v) == AsFloat64(other), nil
	case TypeInteger, TypeBigint:
		return float64(v) == float64(AsInt64(other)), nil
	default:
		return false, nil
	}
}

func (v DoublePrecisionValue) GT(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeDoublePrecision:
		return float64(v) > AsFloat64(other), nil
	case TypeInteger, TypeBigint:
		return float64(v) > float64(AsInt64(other)), nil
	default:
		return false, nil
	}
}

func (v DoublePrecisionValue) GTE(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeDoublePrecision:
		return float64(v) >= AsFloat64(other), nil
	case TypeInteger, TypeBigint:
		return float64(v) >= float64(AsInt64(other)), nil
	default:
		return false, nil
	}
}

func (v DoublePrecisionValue) LT(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeDoublePrecision:
		return float64(v) < AsFloat64(other), nil
	case TypeInteger, TypeBigint:
		return float64(v) < float64(AsInt64(other)), nil
	default:
		return false, nil
	}
}

func (v DoublePrecisionValue) LTE(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeDoublePrecision:
		return float64(v) <= AsFloat64(other), nil
	case TypeInteger, TypeBigint:
		return float64(v) <= float64(AsInt64(other)), nil
	default:
		return false, nil
	}
}

func (v DoublePrecisionValue) Between(a, b Value) (bool, error) {
	if !a.Type().IsNumber() || !b.Type().IsNumber() {
		return false, nil
	}

	ok, err := a.LTE(v)
	if err != nil || !ok {
		return false, err
	}

	return b.GTE(v)
}

func (v DoublePrecisionValue) Add(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeInteger, TypeBigint:
		return NewDoublePrevisionValue(float64(v) + float64(AsInt64(other))), nil
	case TypeDoublePrecision:
		return NewDoublePrevisionValue(float64(v) + AsFloat64(other)), nil
	}

	return NewNullValue(), nil
}

func (v DoublePrecisionValue) Sub(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeInteger, TypeBigint:
		return NewDoublePrevisionValue(float64(v) - float64(AsInt64(other))), nil
	case TypeDoublePrecision:
		return NewDoublePrevisionValue(float64(v) - AsFloat64(other)), nil
	}

	return NewNullValue(), nil
}

func (v DoublePrecisionValue) Mul(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeInteger, TypeBigint:
		return NewDoublePrevisionValue(float64(v) * float64(AsInt64(other))), nil
	case TypeDoublePrecision:
		return NewDoublePrevisionValue(float64(v) * AsFloat64(other)), nil
	}

	return NewNullValue(), nil
}

func (v DoublePrecisionValue) Div(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeInteger, TypeBigint:
		xb := float64(AsInt64(other))
		if xb == 0 {
			return NewNullValue(), nil
		}

		return NewDoublePrevisionValue(float64(v) / xb), nil
	case TypeDoublePrecision:
		xb := AsFloat64(other)
		if xb == 0 {
			return NewNullValue(), nil
		}

		return NewDoublePrevisionValue(float64(v) / xb), nil
	}

	return NewNullValue(), nil
}

func (v DoublePrecisionValue) Mod(other Numeric) (Value, error) {
	switch other.Type() {
	case TypeInteger, TypeBigint:
		xb := float64(AsInt64(other))
		xr := math.Mod(float64(v), xb)
		if math.IsNaN(xr) {
			return NewNullValue(), nil
		}

		return NewDoublePrevisionValue(xr), nil
	case TypeDoublePrecision:
		xb := AsFloat64(other)
		xr := math.Mod(float64(v), xb)
		if math.IsNaN(xr) {
			return NewNullValue(), nil
		}

		return NewDoublePrevisionValue(xr), nil
	}

	return NewNullValue(), nil
}
