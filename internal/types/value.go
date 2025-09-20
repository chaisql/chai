package types

import (
	"fmt"
	"math"
	"time"
)

type Value interface {
	Comparable

	Type() Type
	V() any
	IsZero() (bool, error)
	String() string
	MarshalJSON() ([]byte, error)
	MarshalText() ([]byte, error)
	TypeDef() TypeDefinition
	Encode(dst []byte) ([]byte, error)
	EncodeAsKey(dst []byte) ([]byte, error)
	CastAs(t Type) (Value, error)
}

func AsBool(v Value) bool {
	return v.V().(bool)
}

func AsInt32(v Value) int32 {
	iv, ok := v.(IntegerValue)
	if ok {
		return int32(iv)
	}

	if bv, ok := v.(BigintValue); ok {
		if bv < math.MinInt32 || bv > math.MaxInt32 {
			panic(fmt.Errorf("value %d out of range for int32", bv))
		}
		return int32(bv)
	}

	return v.V().(int32)
}

func AsInt64(v Value) int64 {
	biv, ok := v.(BigintValue)
	if ok {
		return int64(biv)
	}

	iv, ok := v.(IntegerValue)
	if ok {
		return int64(iv)
	}

	return v.V().(int64)
}

func AsFloat64(v Value) float64 {
	dv, ok := v.(DoublePrecisionValue)
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
	bv, ok := v.(ByteaValue)
	if !ok {
		return v.V().([]byte)
	}

	return bv
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

// ValueScanner implements the sql.Scanner interface for Value.
// The src value will be of one of the following types:
//
//	int32
//	int64
//	float64
//	bool
//	[]byte
//	string
//	time.Time
//	nil - for NULL values
type ValueScanner struct {
	V Value
}

func (v *ValueScanner) Scan(src any) error {
	switch t := src.(type) {
	case int32:
		v.V = NewIntegerValue(t)
	case int64:
		v.V = NewBigintValue(t)
	case float64:
		v.V = NewDoublePrevisionValue(t)
	case bool:
		v.V = NewBooleanValue(t)
	case []byte:
		v.V = NewByteaValue(t)
	case string:
		v.V = NewTextValue(t)
	case time.Time:
		v.V = NewTimestampValue(t)
	case nil:
		v.V = NewNullValue()
	default:
		return fmt.Errorf("unexpected type: %T", src)
	}

	return nil
}
