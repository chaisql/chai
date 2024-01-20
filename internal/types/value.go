package types

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/chaisql/chai/internal/stringutil"
	"github.com/cockroachdb/errors"
	"github.com/golang-module/carbon/v2"
)

var (
	epoch   = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	maxTime = math.MaxInt64 - epoch
	minTime = math.MinInt64 + epoch
)

var _ Value = NewNullValue()
var _ Value = NewBooleanValue(false)
var _ Value = NewIntegerValue(0)
var _ Value = NewDoubleValue(0)
var _ Value = NewTextValue("")
var _ Value = NewBlobValue(nil)
var _ Value = NewTimestampValue(time.Time{})
var _ Value = NewArrayValue(nil)
var _ Value = NewObjectValue(nil)

type NullValue struct{}

// NewNullValue returns a SQL BOOLEAN value.
func NewNullValue() NullValue {
	return NullValue{}
}

func (v NullValue) V() any {
	return nil
}

func (v NullValue) Type() ValueType {
	return TypeNull
}

func (v NullValue) IsZero() (bool, error) {
	return false, nil
}

func (v NullValue) String() string {
	return "NULL"
}

func (v NullValue) EQ(other Value) (bool, error) {
	return other.Type() == TypeNull, nil
}

func (v NullValue) GT(other Value) (bool, error) {
	return false, nil
}

func (v NullValue) GTE(other Value) (bool, error) {
	return other.Type() == TypeNull, nil
}

func (v NullValue) LT(other Value) (bool, error) {
	return false, nil
}

func (v NullValue) LTE(other Value) (bool, error) {
	return other.Type() == TypeNull, nil
}

func (v NullValue) Between(a, b Value) (bool, error) {
	return false, nil
}

func (v NullValue) Add(other Value) (Value, error) {
	return v, nil
}

func (v NullValue) Sub(other Value) (Value, error) {
	return v, nil
}

func (v NullValue) Mul(other Value) (Value, error) {
	return v, nil
}

func (v NullValue) Div(other Value) (Value, error) {
	return v, nil
}

func (v NullValue) Mod(other Value) (Value, error) {
	return v, nil
}

func (v NullValue) BitwiseAnd(other Value) (Value, error) {
	return v, nil
}

func (v NullValue) BitwiseOr(other Value) (Value, error) {
	return v, nil
}

func (v NullValue) BitwiseXor(other Value) (Value, error) {
	return v, nil
}

func (v NullValue) MarshalText() ([]byte, error) {
	return []byte("NULL"), nil
}

func (v NullValue) MarshalJSON() ([]byte, error) {
	return []byte("null"), nil
}

type BooleanValue bool

// NewBooleanValue returns a SQL BOOLEAN value.
func NewBooleanValue(x bool) BooleanValue {
	return BooleanValue(x)
}

func (v BooleanValue) V() any {
	return bool(v)
}

func (v BooleanValue) Type() ValueType {
	return TypeBoolean
}

func (v BooleanValue) IsZero() (bool, error) {
	return !bool(v), nil
}

func (v BooleanValue) EQ(other Value) (bool, error) {
	if other.Type() != TypeBoolean {
		return false, nil
	}

	return bool(v) == AsBool(other), nil
}

func (v BooleanValue) GT(other Value) (bool, error) {
	if other.Type() != TypeBoolean {
		return false, nil
	}

	return bool(v) && !AsBool(other), nil
}

func (v BooleanValue) GTE(other Value) (bool, error) {
	if other.Type() != TypeBoolean {
		return false, nil
	}

	bv := bool(v)
	return bv == AsBool(other) || bv, nil
}

func (v BooleanValue) LT(other Value) (bool, error) {
	if other.Type() != TypeBoolean {
		return false, nil
	}

	return !bool(v) && AsBool(other), nil
}

func (v BooleanValue) LTE(other Value) (bool, error) {
	if other.Type() != TypeBoolean {
		return false, nil
	}

	bv := bool(v)
	return bv == AsBool(other) || !bv, nil
}

func (v BooleanValue) Between(a, b Value) (bool, error) {
	if a.Type() != TypeBoolean || b.Type() != TypeBoolean {
		return false, nil
	}

	ok, err := a.LTE(v)
	if err != nil || !ok {
		return false, err
	}

	return b.GTE(v)
}

func (v BooleanValue) Add(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v BooleanValue) Sub(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v BooleanValue) Mul(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v BooleanValue) Div(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v BooleanValue) Mod(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v BooleanValue) BitwiseAnd(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v BooleanValue) BitwiseOr(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v BooleanValue) BitwiseXor(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v BooleanValue) String() string {
	return strconv.FormatBool(bool(v))
}

func (v BooleanValue) MarshalText() ([]byte, error) {
	return []byte(strconv.FormatBool(bool(v))), nil
}

func (v BooleanValue) MarshalJSON() ([]byte, error) {
	return v.MarshalText()
}

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

func (v IntegerValue) Add(other Value) (Value, error) {
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

func (v IntegerValue) Sub(other Value) (Value, error) {
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

func (v IntegerValue) Mul(other Value) (Value, error) {
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

func (v IntegerValue) Div(other Value) (Value, error) {
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

func (v IntegerValue) Mod(other Value) (Value, error) {
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

func (v IntegerValue) BitwiseAnd(other Value) (Value, error) {
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

func (v IntegerValue) BitwiseOr(other Value) (Value, error) {
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

func (v IntegerValue) BitwiseXor(other Value) (Value, error) {
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

func (v IntegerValue) String() string {
	return strconv.FormatInt(int64(v), 10)
}

func (v IntegerValue) MarshalText() ([]byte, error) {
	return []byte(strconv.FormatInt(int64(v), 10)), nil
}

func (v IntegerValue) MarshalJSON() ([]byte, error) {
	return v.MarshalText()
}

type DoubleValue float64

// NewDoubleValue returns a SQL DOUBLE value.
func NewDoubleValue(x float64) DoubleValue {
	return DoubleValue(x)
}

func (v DoubleValue) V() any {
	return float64(v)
}

func (v DoubleValue) Type() ValueType {
	return TypeDouble
}

func (v DoubleValue) IsZero() (bool, error) {
	return v == 0, nil
}

func (v DoubleValue) EQ(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeDouble:
		return float64(v) == AsFloat64(other), nil
	case TypeInteger:
		return float64(v) == float64(AsInt64(other)), nil
	default:
		return false, nil
	}
}

func (v DoubleValue) GT(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeDouble:
		return float64(v) > AsFloat64(other), nil
	case TypeInteger:
		return float64(v) > float64(AsInt64(other)), nil
	default:
		return false, nil
	}
}

func (v DoubleValue) GTE(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeDouble:
		return float64(v) >= AsFloat64(other), nil
	case TypeInteger:
		return float64(v) >= float64(AsInt64(other)), nil
	default:
		return false, nil
	}
}

func (v DoubleValue) LT(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeDouble:
		return float64(v) < AsFloat64(other), nil
	case TypeInteger:
		return float64(v) < float64(AsInt64(other)), nil
	default:
		return false, nil
	}
}

func (v DoubleValue) LTE(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeDouble:
		return float64(v) <= AsFloat64(other), nil
	case TypeInteger:
		return float64(v) <= float64(AsInt64(other)), nil
	default:
		return false, nil
	}
}

func (v DoubleValue) Between(a, b Value) (bool, error) {
	if !a.Type().IsNumber() || !b.Type().IsNumber() {
		return false, nil
	}

	ok, err := a.LTE(v)
	if err != nil || !ok {
		return false, err
	}

	return b.GTE(v)
}

func (v DoubleValue) Add(other Value) (Value, error) {
	switch other.Type() {
	case TypeInteger:
		return NewDoubleValue(float64(v) + float64(AsInt64(other))), nil
	case TypeDouble:
		return NewDoubleValue(float64(v) + AsFloat64(other)), nil
	}

	return NewNullValue(), nil
}

func (v DoubleValue) Sub(other Value) (Value, error) {
	switch other.Type() {
	case TypeInteger:
		return NewDoubleValue(float64(v) - float64(AsInt64(other))), nil
	case TypeDouble:
		return NewDoubleValue(float64(v) - AsFloat64(other)), nil
	}

	return NewNullValue(), nil
}

func (v DoubleValue) Mul(other Value) (Value, error) {
	switch other.Type() {
	case TypeInteger:
		return NewDoubleValue(float64(v) * float64(AsInt64(other))), nil
	case TypeDouble:
		return NewDoubleValue(float64(v) * AsFloat64(other)), nil
	}

	return NewNullValue(), nil
}

func (v DoubleValue) Div(other Value) (Value, error) {
	switch other.Type() {
	case TypeInteger:
		xb := float64(AsInt64(other))
		if xb == 0 {
			return NewNullValue(), nil
		}

		return NewDoubleValue(float64(v) / xb), nil
	case TypeDouble:
		xb := AsFloat64(other)
		if xb == 0 {
			return NewNullValue(), nil
		}

		return NewDoubleValue(float64(v) / xb), nil
	}

	return NewNullValue(), nil
}

func (v DoubleValue) Mod(other Value) (Value, error) {
	switch other.Type() {
	case TypeInteger:
		xb := float64(AsInt64(other))
		xr := math.Mod(float64(v), xb)
		if math.IsNaN(xr) {
			return NewNullValue(), nil
		}

		return NewDoubleValue(xr), nil
	case TypeDouble:
		xb := AsFloat64(other)
		xr := math.Mod(float64(v), xb)
		if math.IsNaN(xr) {
			return NewNullValue(), nil
		}

		return NewDoubleValue(xr), nil
	}

	return NewNullValue(), nil
}

func (v DoubleValue) BitwiseAnd(other Value) (Value, error) {
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

func (v DoubleValue) BitwiseOr(other Value) (Value, error) {
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

func (v DoubleValue) BitwiseXor(other Value) (Value, error) {
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

func (v DoubleValue) String() string {
	f := As[float64](v)
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
	return strconv.FormatFloat(As[float64](v), fmt, prec, 64)
}

func (v DoubleValue) MarshalText() ([]byte, error) {
	return []byte(v.String()), nil
}

func (v DoubleValue) MarshalJSON() ([]byte, error) {
	f := As[float64](v)
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
	return strconv.AppendFloat(nil, As[float64](v), fmt, prec, 64), nil
}

type TimestampValue time.Time

// NewTimestampValue returns a SQL TIMESTAMP value.
func NewTimestampValue(x time.Time) TimestampValue {
	return TimestampValue(x.UTC())
}

func (v TimestampValue) V() any {
	return time.Time(v)
}

func (v TimestampValue) Type() ValueType {
	return TypeTimestamp
}

func (v TimestampValue) IsZero() (bool, error) {
	return time.Time(v).IsZero(), nil
}

func (v TimestampValue) EQ(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeTimestamp:
		return time.Time(v).Equal(AsTime(other)), nil
	case TypeText:
		ts, err := ParseTimestamp(AsString(other))
		if err != nil {
			return false, err
		}
		return time.Time(v).Equal(ts), nil
	default:
		return false, nil
	}
}

func (v TimestampValue) GT(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeTimestamp:
		return time.Time(v).After(AsTime(other)), nil
	case TypeText:
		ts, err := ParseTimestamp(AsString(other))
		if err != nil {
			return false, err
		}
		return time.Time(v).After(ts), nil
	default:
		return false, nil
	}
}

func (v TimestampValue) GTE(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeTimestamp:
		ta := time.Time(v)
		tb := AsTime(other)
		return ta.After(tb) || ta.Equal(tb), nil
	case TypeText:
		ta := time.Time(v)
		tb, err := ParseTimestamp(AsString(other))
		if err != nil {
			return false, err
		}

		return ta.After(tb) || ta.Equal(tb), nil
	default:
		return false, nil
	}
}

func (v TimestampValue) LT(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeTimestamp:
		return time.Time(v).Before(AsTime(other)), nil
	case TypeText:
		ts, err := ParseTimestamp(AsString(other))
		if err != nil {
			return false, err
		}
		return time.Time(v).Before(ts), nil
	default:
		return false, nil
	}
}

func (v TimestampValue) LTE(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeTimestamp:
		ta := time.Time(v)
		tb := AsTime(other)
		return ta.Before(tb) || ta.Equal(tb), nil
	case TypeText:
		ta := time.Time(v)
		tb, err := ParseTimestamp(AsString(other))
		if err != nil {
			return false, err
		}

		return ta.Before(tb) || ta.Equal(tb), nil
	default:
		return false, nil
	}
}

func (v TimestampValue) Between(a, b Value) (bool, error) {
	if !a.Type().IsTimestampCompatible() || !b.Type().IsTimestampCompatible() {
		return false, nil
	}

	ok, err := a.LTE(v)
	if err != nil || !ok {
		return false, err
	}

	return b.GTE(v)
}

func (v TimestampValue) Add(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v TimestampValue) Sub(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v TimestampValue) Mul(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v TimestampValue) Div(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v TimestampValue) Mod(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v TimestampValue) BitwiseAnd(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v TimestampValue) BitwiseOr(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v TimestampValue) BitwiseXor(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v TimestampValue) String() string {
	return strconv.Quote(time.Time(v).Format(time.RFC3339Nano))
}

func (v TimestampValue) MarshalText() ([]byte, error) {
	return []byte(v.String()), nil
}

func (v TimestampValue) MarshalJSON() ([]byte, error) {
	return v.MarshalText()
}

type TextValue string

// NewTextValue returns a SQL TEXT value.
func NewTextValue(x string) TextValue {
	return TextValue(x)
}

func (v TextValue) V() any {
	return string(v)
}

func (v TextValue) Type() ValueType {
	return TypeText
}

func (v TextValue) IsZero() (bool, error) {
	return v == "", nil
}

func (v TextValue) EQ(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeText:
		return strings.Compare(string(v), AsString(other)) == 0, nil
	case TypeTimestamp:
		ts, err := ParseTimestamp(As[string](v))
		if err != nil {
			return false, err
		}
		return ts.Equal(AsTime(other)), nil
	default:
		return false, nil
	}
}

func (v TextValue) GT(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeText:
		return strings.Compare(string(v), AsString(other)) > 0, nil
	case TypeTimestamp:
		ts, err := ParseTimestamp(As[string](v))
		if err != nil {
			return false, err
		}
		return ts.After(AsTime(other)), nil
	default:
		return false, nil
	}
}

func (v TextValue) GTE(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeText:
		return strings.Compare(string(v), AsString(other)) >= 0, nil
	case TypeTimestamp:
		t1, err := ParseTimestamp(As[string](v))
		if err != nil {
			return false, err
		}
		t2 := AsTime(other)
		return t1.After(t2) || t1.Equal(t2), nil
	default:
		return false, nil
	}
}

func (v TextValue) LT(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeText:
		return strings.Compare(string(v), AsString(other)) < 0, nil
	case TypeTimestamp:
		ts, err := ParseTimestamp(As[string](v))
		if err != nil {
			return false, err
		}
		return ts.Before(AsTime(other)), nil
	default:
		return false, nil
	}
}

func (v TextValue) LTE(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeText:
		return strings.Compare(string(v), AsString(other)) <= 0, nil
	case TypeTimestamp:
		t1, err := ParseTimestamp(As[string](v))
		if err != nil {
			return false, err
		}
		t2 := AsTime(other)
		return t1.Before(t2) || t1.Equal(t2), nil
	default:
		return false, nil
	}
}

func (v TextValue) Between(a, b Value) (bool, error) {
	if a.Type() != TypeText || b.Type() != TypeText {
		return false, nil
	}

	ok, err := a.LTE(v)
	if err != nil || !ok {
		return false, err
	}

	return b.GTE(v)
}

func (v TextValue) Add(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v TextValue) Sub(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v TextValue) Mul(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v TextValue) Div(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v TextValue) Mod(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v TextValue) BitwiseAnd(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v TextValue) BitwiseOr(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v TextValue) BitwiseXor(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v TextValue) String() string {
	return strconv.Quote(string(v))
}

func (v TextValue) MarshalText() ([]byte, error) {
	return []byte(v.String()), nil
}

func (v TextValue) MarshalJSON() ([]byte, error) {
	return v.MarshalText()
}

type BlobValue []byte

// NewBlobValue returns a SQL BLOB value.
func NewBlobValue(x []byte) BlobValue {
	return BlobValue(x)
}

func (v BlobValue) V() any {
	return []byte(v)
}

func (v BlobValue) Type() ValueType {
	return TypeBlob
}

func (v BlobValue) IsZero() (bool, error) {
	return v == nil, nil
}

func (v BlobValue) EQ(other Value) (bool, error) {
	if other.Type() != TypeBlob {
		return false, nil
	}

	return bytes.Equal([]byte(v), AsByteSlice(other)), nil
}

func (v BlobValue) GT(other Value) (bool, error) {
	t := other.Type()
	if t != TypeBlob {
		return false, nil
	}

	return bytes.Compare([]byte(v), AsByteSlice(other)) > 0, nil
}

func (v BlobValue) GTE(other Value) (bool, error) {
	t := other.Type()
	if t != TypeBlob {
		return false, nil
	}

	return bytes.Compare([]byte(v), AsByteSlice(other)) >= 0, nil
}

func (v BlobValue) LT(other Value) (bool, error) {
	t := other.Type()
	if t != TypeBlob {
		return false, nil
	}

	return bytes.Compare([]byte(v), AsByteSlice(other)) < 0, nil
}

func (v BlobValue) LTE(other Value) (bool, error) {
	t := other.Type()
	if t != TypeBlob {
		return false, nil
	}

	return bytes.Compare([]byte(v), AsByteSlice(other)) <= 0, nil
}

func (v BlobValue) Between(a, b Value) (bool, error) {
	if a.Type() != TypeBlob || b.Type() != TypeBlob {
		return false, nil
	}

	ok, err := a.LTE(v)
	if err != nil || !ok {
		return false, err
	}

	return b.GTE(v)
}

func (v BlobValue) Add(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v BlobValue) Sub(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v BlobValue) Mul(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v BlobValue) Div(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v BlobValue) Mod(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v BlobValue) BitwiseAnd(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v BlobValue) BitwiseOr(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v BlobValue) BitwiseXor(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v BlobValue) String() string {
	t, _ := v.MarshalText()
	return string(t)
}

func (v BlobValue) MarshalText() ([]byte, error) {
	var dst bytes.Buffer
	dst.WriteString("\"\\x")
	_, _ = hex.NewEncoder(&dst).Write(v)
	dst.WriteByte('"')
	return dst.Bytes(), nil
}

func (v BlobValue) MarshalJSON() ([]byte, error) {
	dst := make([]byte, base64.StdEncoding.EncodedLen(len(v))+2)
	dst[0] = '"'
	dst[len(dst)-1] = '"'
	base64.StdEncoding.Encode(dst[1:], v)
	return dst, nil
}

type ArrayValue struct {
	a Array
}

// NewArrayValue returns a SQL ARRAY value.
func NewArrayValue(x Array) *ArrayValue {
	return &ArrayValue{
		a: x,
	}
}

func (v *ArrayValue) V() any {
	return v.a
}

func (v *ArrayValue) Type() ValueType {
	return TypeArray
}

func (v *ArrayValue) IsZero() (bool, error) {
	// The zero value of an array is an empty array.
	// Thus, if GetByIndex(0) returns the ErrValueNotFound
	// it means that the array is empty.
	_, err := v.a.GetByIndex(0)
	if errors.Is(err, ErrValueNotFound) {
		return true, nil
	}
	return false, err
}

func (v *ArrayValue) EQ(other Value) (bool, error) {
	t := other.Type()
	if t != TypeArray {
		return false, nil
	}

	return compareArrays(operatorEq, v.a, AsArray(other))
}

func (v *ArrayValue) GT(other Value) (bool, error) {
	t := other.Type()
	if t != TypeArray {
		return false, nil
	}

	return compareArrays(operatorGt, v.a, AsArray(other))
}

func (v *ArrayValue) GTE(other Value) (bool, error) {
	t := other.Type()
	if t != TypeArray {
		return false, nil
	}

	return compareArrays(operatorGte, v.a, AsArray(other))
}

func (v *ArrayValue) LT(other Value) (bool, error) {
	t := other.Type()
	if t != TypeArray {
		return false, nil
	}

	return compareArrays(operatorLt, v.a, AsArray(other))
}

func (v *ArrayValue) LTE(other Value) (bool, error) {
	t := other.Type()
	if t != TypeArray {
		return false, nil
	}

	return compareArrays(operatorLte, v.a, AsArray(other))
}

func (v *ArrayValue) Between(a, b Value) (bool, error) {
	if a.Type() != TypeArray || b.Type() != TypeArray {
		return false, nil
	}

	ok, err := a.LTE(v)
	if err != nil || !ok {
		return false, err
	}

	return b.GTE(v)
}

func (v *ArrayValue) Add(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v *ArrayValue) Sub(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v *ArrayValue) Mul(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v *ArrayValue) Div(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v *ArrayValue) Mod(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v *ArrayValue) BitwiseAnd(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v *ArrayValue) BitwiseOr(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v *ArrayValue) BitwiseXor(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v *ArrayValue) String() string {
	data, _ := v.MarshalText()
	return string(data)
}

func (v *ArrayValue) MarshalText() ([]byte, error) {
	return MarshalTextIndent(v, "", "")
}

func (v *ArrayValue) MarshalJSON() ([]byte, error) {
	return jsonArray{Array: v.a}.MarshalJSON()
}

type ObjectValue struct {
	o Object
}

// NewObjectValue returns a SQL INTEGER value.
func NewObjectValue(x Object) *ObjectValue {
	return &ObjectValue{
		o: x,
	}
}

func (o *ObjectValue) V() any {
	return o.o
}

func (o *ObjectValue) Type() ValueType {
	return TypeObject
}

func (v *ObjectValue) IsZero() (bool, error) {
	err := v.o.Iterate(func(_ string, _ Value) error {
		// We return an error in the first iteration to stop it.
		return errors.WithStack(errStop)
	})
	if err == nil {
		// If err is nil, it means that we didn't iterate,
		// thus the object is empty.
		return true, nil
	}
	if errors.Is(err, errStop) {
		// If err is errStop, it means that we iterate
		// at least once, thus the object is not empty.
		return false, nil
	}
	// An unexpecting error occurs, let's return it!
	return false, err
}

func (v *ObjectValue) EQ(other Value) (bool, error) {
	t := other.Type()
	if t != TypeObject {
		return false, nil
	}

	return compareObjects(operatorEq, v.o, AsObject(other))
}

func (v *ObjectValue) GT(other Value) (bool, error) {
	t := other.Type()
	if t != TypeObject {
		return false, nil
	}

	return compareObjects(operatorGt, v.o, AsObject(other))
}

func (v *ObjectValue) GTE(other Value) (bool, error) {
	t := other.Type()
	if t != TypeObject {
		return false, nil
	}

	return compareObjects(operatorGte, v.o, AsObject(other))
}

func (v *ObjectValue) LT(other Value) (bool, error) {
	t := other.Type()
	if t != TypeObject {
		return false, nil
	}

	return compareObjects(operatorLt, v.o, AsObject(other))
}

func (v *ObjectValue) LTE(other Value) (bool, error) {
	t := other.Type()
	if t != TypeObject {
		return false, nil
	}

	return compareObjects(operatorLte, v.o, AsObject(other))
}

func (v *ObjectValue) Between(a, b Value) (bool, error) {
	if a.Type() != TypeObject || b.Type() != TypeObject {
		return false, nil
	}

	ok, err := a.LTE(v)
	if err != nil || !ok {
		return false, err
	}

	return b.GTE(v)
}

func (v *ObjectValue) Add(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v *ObjectValue) Sub(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v *ObjectValue) Mul(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v *ObjectValue) Div(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v *ObjectValue) Mod(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v *ObjectValue) BitwiseAnd(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v *ObjectValue) BitwiseOr(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (v *ObjectValue) BitwiseXor(other Value) (Value, error) {
	return NewNullValue(), nil
}

func (o *ObjectValue) String() string {
	data, _ := o.MarshalText()
	return string(data)
}

func (o *ObjectValue) MarshalText() ([]byte, error) {
	return MarshalTextIndent(o, "", "")
}

func (o *ObjectValue) MarshalJSON() ([]byte, error) {
	return jsonObject{Object: o.o}.MarshalJSON()
}

func As[T any](v Value) T {
	return v.V().(T)
}

func AsBool(v Value) bool {
	bv, ok := v.(BooleanValue)
	if !ok {
		return v.V().(bool)
	}

	return bool(bv)
}

func AsInt64(v Value) int64 {
	iv, ok := v.(IntegerValue)
	if !ok {
		return v.V().(int64)
	}

	return int64(iv)
}

func AsFloat64(v Value) float64 {
	dv, ok := v.(DoubleValue)
	if !ok {
		return v.V().(float64)
	}

	return float64(dv)
}

func AsTime(v Value) time.Time {
	tv, ok := v.(TimestampValue)
	if !ok {
		return v.V().(time.Time)
	}

	return time.Time(tv)
}

func AsString(v Value) string {
	tv, ok := v.(TextValue)
	if !ok {
		return v.V().(string)
	}

	return string(tv)
}

func AsByteSlice(v Value) []byte {
	bv, ok := v.(BlobValue)
	if !ok {
		return v.V().([]byte)
	}

	return bv
}

func AsArray(v Value) Array {
	av, ok := v.(*ArrayValue)
	if !ok {
		return v.V().(Array)
	}

	return av.a
}

func AsObject(v Value) Object {
	ov, ok := v.(*ObjectValue)
	if !ok {
		return v.V().(Object)
	}

	return ov.o
}

func Is[T any](v Value) (T, bool) {
	x, ok := v.V().(T)
	return x, ok
}

func IsNull(v Value) bool {
	return v == nil || v.Type() == TypeNull
}

// IsTruthy returns whether v is not Equal to the zero value of its type.
func IsTruthy(v Value) (bool, error) {
	if v.Type() == TypeNull {
		return false, nil
	}

	b, err := v.IsZero()
	return !b, err
}

func MarshalTextIndent(v Value, prefix, indent string) ([]byte, error) {
	var buf bytes.Buffer

	err := marshalText(&buf, v, prefix, indent, 0)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func marshalText(dst *bytes.Buffer, v Value, prefix, indent string, depth int) error {
	if v.V() == nil {
		dst.WriteString("NULL")
		return nil
	}

	switch v.Type() {
	case TypeNull:
		dst.WriteString("NULL")
		return nil
	case TypeBoolean:
		dst.WriteString(strconv.FormatBool(As[bool](v)))
		return nil
	case TypeInteger:
		dst.WriteString(strconv.FormatInt(As[int64](v), 10))
		return nil
	case TypeDouble:
		f := As[float64](v)
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
		dst.WriteString(strconv.FormatFloat(As[float64](v), fmt, prec, 64))
		return nil
	case TypeTimestamp:
		dst.WriteString(strconv.Quote(As[time.Time](v).Format(time.RFC3339Nano)))
		return nil
	case TypeText:
		dst.WriteString(strconv.Quote(As[string](v)))
		return nil
	case TypeBlob:
		src := As[[]byte](v)
		dst.WriteString("\"\\x")
		hex.NewEncoder(dst).Write(src)
		dst.WriteByte('"')
		return nil
	case TypeArray:
		var nonempty bool
		dst.WriteByte('[')
		err := As[Array](v).Iterate(func(i int, value Value) error {
			nonempty = true
			if i > 0 {
				dst.WriteByte(',')
				if prefix == "" {
					dst.WriteByte(' ')
				}
			}
			newline(dst, prefix, indent, depth+1)

			return marshalText(dst, value, prefix, indent, depth+1)
		})
		if err != nil {
			return err
		}
		if nonempty && prefix != "" {
			newline(dst, prefix, indent, depth)
		}
		dst.WriteByte(']')
		return nil
	case TypeObject:
		dst.WriteByte('{')
		var i int
		err := As[Object](v).Iterate(func(field string, value Value) error {
			if i > 0 {
				dst.WriteByte(',')
				if prefix == "" {
					dst.WriteByte(' ')
				}
			}
			newline(dst, prefix, indent, depth+1)
			i++

			var ident string
			if strings.HasPrefix(field, "\"") {
				ident = stringutil.NormalizeIdentifier(field, '`')
			} else {
				ident = stringutil.NormalizeIdentifier(field, '"')
			}
			dst.WriteString(ident)
			dst.WriteString(": ")

			return marshalText(dst, value, prefix, indent, depth+1)
		})
		if err != nil {
			return err
		}
		newline(dst, prefix, indent, depth)
		dst.WriteRune('}')
		return nil
	default:
		return fmt.Errorf("unexpected type: %d", v.Type())
	}
}

func newline(dst *bytes.Buffer, prefix, indent string, depth int) {
	dst.WriteString(prefix)
	for i := 0; i < depth; i++ {
		dst.WriteString(indent)
	}
}

type jsonArray struct {
	Array
}

func (j jsonArray) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteRune('[')
	err := j.Array.Iterate(func(i int, v Value) error {
		if i > 0 {
			buf.WriteString(", ")
		}

		data, err := v.MarshalJSON()
		if err != nil {
			return err
		}

		_, err = buf.Write(data)
		return err
	})
	if err != nil {
		return nil, err
	}
	buf.WriteRune(']')

	return buf.Bytes(), nil
}

type jsonObject struct {
	Object
}

func (j jsonObject) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteByte('{')

	var notFirst bool
	err := j.Object.Iterate(func(f string, v Value) error {
		if notFirst {
			buf.WriteString(", ")
		}
		notFirst = true

		buf.WriteString(strconv.Quote(f))
		buf.WriteString(": ")

		data, err := v.MarshalJSON()
		if err != nil {
			return err
		}
		_, err = buf.Write(data)
		return err
	})
	if err != nil {
		return nil, err
	}

	buf.WriteByte('}')

	return buf.Bytes(), nil
}

func ParseTimestamp(s string) (time.Time, error) {
	c := carbon.Parse(s, "UTC")
	if c.Error != nil {
		return time.Time{}, errors.New("invalid timestamp")
	}

	ts := c.ToStdTime()
	m := ts.UnixMicro()
	if m > maxTime || m < minTime {
		return time.Time{}, errors.New("timestamp out of range")
	}

	return ts, nil
}
