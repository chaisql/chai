package genji

import "github.com/asdine/genji/value"

// BytesValue creates a litteral value of type Bytes.
func BytesValue(v []byte) LitteralValue {
	return LitteralValue{value.NewBytes(v)}
}

// StringValue creates a litteral value of type String.
func StringValue(v string) LitteralValue {
	return LitteralValue{value.NewString(v)}
}

// BoolValue creates a litteral value of type Bool.
func BoolValue(v bool) LitteralValue {
	return LitteralValue{value.NewBool(v)}
}

// UintValue creates a litteral value of type Uint.
func UintValue(v uint) LitteralValue {
	return LitteralValue{value.NewUint(v)}
}

// Uint8Value creates a litteral value of type Uint8.
func Uint8Value(v uint8) LitteralValue {
	return LitteralValue{value.NewUint8(v)}
}

// Uint16Value creates a litteral value of type Uint16.
func Uint16Value(v uint16) LitteralValue {
	return LitteralValue{value.NewUint16(v)}
}

// Uint32Value creates a litteral value of type Uint32.
func Uint32Value(v uint32) LitteralValue {
	return LitteralValue{value.NewUint32(v)}
}

// Uint64Value creates a litteral value of type Uint64.
func Uint64Value(v uint64) LitteralValue {
	return LitteralValue{value.NewUint64(v)}
}

// IntValue creates a litteral value of type Int.
func IntValue(v int) LitteralValue {
	return LitteralValue{value.NewInt(v)}
}

// Int8Value creates a litteral value of type Int8.
func Int8Value(v int8) LitteralValue {
	return LitteralValue{value.NewInt8(v)}
}

// Int16Value creates a litteral value of type Int16.
func Int16Value(v int16) LitteralValue {
	return LitteralValue{value.NewInt16(v)}
}

// Int32Value creates a litteral value of type Int32.
func Int32Value(v int32) LitteralValue {
	return LitteralValue{value.NewInt32(v)}
}

// Int64Value creates a litteral value of type Int64.
func Int64Value(v int64) LitteralValue {
	return LitteralValue{value.NewInt64(v)}
}

// Float32Value creates a litteral value of type Float32.
func Float32Value(v float32) LitteralValue {
	return LitteralValue{value.NewFloat32(v)}
}

// Float64Value creates a litteral value of type Float64.
func Float64Value(v float64) LitteralValue {
	return LitteralValue{value.NewFloat64(v)}
}
