// Package field defines types to manipulate and encode fields.
package field

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// Type represents a field type supported by the database.
type Type uint8

// List of supported field types.
const (
	Bytes Type = iota + 1
	String
	Bool
	Uint
	Uint8
	Uint16
	Uint32
	Uint64
	Int
	Int8
	Int16
	Int32
	Int64
	Float32
	Float64
)

func (t Type) String() string {
	switch t {
	case Bytes:
		return "Bytes"
	case String:
		return "String"
	case Bool:
		return "Bool"
	case Uint:
		return "Uint"
	case Uint8:
		return "Uint8"
	case Uint16:
		return "Uint16"
	case Uint32:
		return "Uint32"
	case Uint64:
		return "Uint64"
	case Int:
		return "Int"
	case Int8:
		return "Int8"
	case Int16:
		return "Int16"
	case Int32:
		return "Int32"
	case Int64:
		return "Int64"
	case Float32:
		return "Float32"
	case Float64:
		return "Float64"
	}

	return ""
}

// TypeFromGoType returns the Type corresponding to the given Go type.
func TypeFromGoType(tp string) Type {
	switch tp {
	case "[]byte":
		return Bytes
	case "string":
		return String
	case "bool":
		return Bool
	case "uint":
		return Uint
	case "uint8":
		return Uint8
	case "uint16":
		return Uint16
	case "uint32":
		return Uint32
	case "uint64":
		return Uint64
	case "int":
		return Int
	case "int8":
		return Int8
	case "int16":
		return Int16
	case "int32":
		return Int32
	case "int64":
		return Int64
	case "float32":
		return Float32
	case "float64":
		return Float64
	}

	return 0
}

// A Field is a typed information stored in the database.
type Field struct {
	Name string
	Type Type
	Data []byte
}

// New creates a field whose type is infered from x.
func New(name string, x interface{}) (Field, error) {
	switch v := x.(type) {
	case []byte:
		return NewBytes(name, v), nil
	case string:
		return NewString(name, v), nil
	case bool:
		return NewBool(name, v), nil
	case uint:
		return NewUint(name, v), nil
	case uint8:
		return NewUint8(name, v), nil
	case uint16:
		return NewUint16(name, v), nil
	case uint32:
		return NewUint32(name, v), nil
	case uint64:
		return NewUint64(name, v), nil
	case int:
		return NewInt(name, v), nil
	case int8:
		return NewInt8(name, v), nil
	case int16:
		return NewInt16(name, v), nil
	case int32:
		return NewInt32(name, v), nil
	case int64:
		return NewInt64(name, v), nil
	case float32:
		return NewFloat32(name, v), nil
	case float64:
		return NewFloat64(name, v), nil
	default:
		return Field{}, fmt.Errorf("unsupported type %t", x)
	}
}

// NewBytes encodes x and returns a field.
func NewBytes(name string, x []byte) Field {
	return Field{
		Name: name,
		Type: Bytes,
		Data: x,
	}
}

// NewString encodes x and returns a field.
func NewString(name string, x string) Field {
	return Field{
		Name: name,
		Type: String,
		Data: []byte(x),
	}
}

// NewBool encodes x and returns a field.
func NewBool(name string, x bool) Field {
	return Field{
		Name: name,
		Type: Bool,
		Data: EncodeBool(x),
	}
}

// NewUint encodes x and returns a field.
func NewUint(name string, x uint) Field {
	return Field{
		Name: name,
		Type: Uint,
		Data: EncodeUint(x),
	}
}

// NewUint8 encodes x and returns a field.
func NewUint8(name string, x uint8) Field {
	return Field{
		Name: name,
		Type: Uint8,
		Data: EncodeUint8(x),
	}
}

// NewUint16 encodes x and returns a field.
func NewUint16(name string, x uint16) Field {
	return Field{
		Name: name,
		Type: Uint16,
		Data: EncodeUint16(x),
	}
}

// NewUint32 encodes x and returns a field.
func NewUint32(name string, x uint32) Field {
	return Field{
		Name: name,
		Type: Uint32,
		Data: EncodeUint32(x),
	}
}

// NewUint64 encodes x and returns a field.
func NewUint64(name string, x uint64) Field {
	return Field{
		Name: name,
		Type: Uint64,
		Data: EncodeUint64(x),
	}
}

// NewInt encodes x and returns a field.
func NewInt(name string, x int) Field {
	return Field{
		Name: name,
		Type: Int,
		Data: EncodeInt(x),
	}
}

// NewInt8 encodes x and returns a field.
func NewInt8(name string, x int8) Field {
	return Field{
		Name: name,
		Type: Int8,
		Data: EncodeInt8(x),
	}
}

// NewInt16 encodes x and returns a field.
func NewInt16(name string, x int16) Field {
	return Field{
		Name: name,
		Type: Int16,
		Data: EncodeInt16(x),
	}
}

// NewInt32 encodes x and returns a field.
func NewInt32(name string, x int32) Field {
	return Field{
		Name: name,
		Type: Int32,
		Data: EncodeInt32(x),
	}
}

// NewInt64 encodes x and returns a field.
func NewInt64(name string, x int64) Field {
	return Field{
		Name: name,
		Type: Int64,
		Data: EncodeInt64(x),
	}
}

// NewFloat32 encodes x and returns a field.
func NewFloat32(name string, x float32) Field {
	return Field{
		Name: name,
		Type: Float32,
		Data: EncodeFloat32(x),
	}
}

// NewFloat64 encodes x and returns a field.
func NewFloat64(name string, x float64) Field {
	return Field{
		Name: name,
		Type: Float64,
		Data: EncodeFloat64(x),
	}
}

// EncodeBytes takes a bytes and returns it.
// It is present to ease code generation.
func EncodeBytes(x []byte) []byte {
	return x
}

// DecodeBytes takes a byte slice and returns it.
// It is present to ease code generation.
func DecodeBytes(buf []byte) ([]byte, error) {
	return buf, nil
}

// EncodeString takes a string and returns its binary representation.
func EncodeString(x string) []byte {
	return []byte(x)
}

// DecodeString takes a byte slice and decodes it into a string.
func DecodeString(buf []byte) (string, error) {
	return string(buf), nil
}

// EncodeBool takes a bool and returns its binary representation.
func EncodeBool(x bool) []byte {
	if x {
		return []byte{1}
	}
	return []byte{0}
}

// DecodeBool takes a byte slice and decodes it into a boolean.
func DecodeBool(buf []byte) (bool, error) {
	if len(buf) != 1 {
		return false, errors.New("cannot decode buffer to bool")
	}
	return buf[0] == 1, nil
}

// EncodeUint takes an uint and returns its binary representation.
func EncodeUint(x uint) []byte {
	return EncodeUint64(uint64(x))
}

// DecodeUint takes a byte slice and decodes it into a uint.
func DecodeUint(buf []byte) (uint, error) {
	x, err := DecodeUint64(buf)
	return uint(x), err
}

// EncodeUint8 takes an uint8 and returns its binary representation.
func EncodeUint8(x uint8) []byte {
	return []byte{x}
}

// DecodeUint8 takes a byte slice and decodes it into a uint8.
func DecodeUint8(buf []byte) (uint8, error) {
	if len(buf) == 0 {
		return 0, errors.New("cannot decode buffer to uint8")
	}

	return buf[0], nil
}

// EncodeUint16 takes an uint16 and returns its binary representation.
func EncodeUint16(x uint16) []byte {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], x)
	return buf[:]
}

// DecodeUint16 takes a byte slice and decodes it into a uint16.
func DecodeUint16(buf []byte) (uint16, error) {
	if len(buf) < 2 {
		return 0, errors.New("cannot decode buffer to uint16")
	}

	return binary.BigEndian.Uint16(buf), nil
}

// EncodeUint32 takes an uint32 and returns its binary representation.
func EncodeUint32(x uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], x)
	return buf[:]
}

// DecodeUint32 takes a byte slice and decodes it into a uint32.
func DecodeUint32(buf []byte) (uint32, error) {
	if len(buf) < 4 {
		return 0, errors.New("cannot decode buffer to uint32")
	}

	return binary.BigEndian.Uint32(buf), nil
}

// EncodeUint64 takes an uint64 and returns its binary representation.
func EncodeUint64(x uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], x)
	return buf[:]
}

// DecodeUint64 takes a byte slice and decodes it into a uint64.
func DecodeUint64(buf []byte) (uint64, error) {
	if len(buf) < 8 {
		return 0, errors.New("cannot decode buffer to uint64")
	}

	return binary.BigEndian.Uint64(buf), nil
}

// EncodeInt takes an int and returns its binary representation.
func EncodeInt(x int) []byte {
	return EncodeInt64(int64(x))
}

// DecodeInt takes a byte slice and decodes it into an int.
func DecodeInt(buf []byte) (int, error) {
	x, err := DecodeInt64(buf)
	return int(x), err
}

// EncodeInt8 takes an int8 and returns its binary representation.
func EncodeInt8(x int8) []byte {
	return []byte{uint8(x + math.MaxInt8 + 1)}
}

// DecodeInt8 takes a byte slice and decodes it into an int8.
func DecodeInt8(buf []byte) (int8, error) {
	return int8(buf[0] - math.MaxInt8 - 1), nil
}

// EncodeInt16 takes an int16 and returns its binary representation.
func EncodeInt16(x int16) []byte {
	var buf [2]byte

	binary.BigEndian.PutUint16(buf[:], uint16(x)+math.MaxInt16+1)
	return buf[:]
}

// DecodeInt16 takes a byte slice and decodes it into an int16.
func DecodeInt16(buf []byte) (int16, error) {
	x, err := DecodeUint16(buf)
	x -= math.MaxInt16 + 1
	return int16(x), err
}

// EncodeInt32 takes an int32 and returns its binary representation.
func EncodeInt32(x int32) []byte {
	var buf [4]byte

	binary.BigEndian.PutUint32(buf[:], uint32(x)+math.MaxInt32+1)
	return buf[:]
}

// DecodeInt32 takes a byte slice and decodes it into an int32.
func DecodeInt32(buf []byte) (int32, error) {
	x, err := DecodeUint32(buf)
	x -= math.MaxInt32 + 1
	return int32(x), err
}

// EncodeInt64 takes an int64 and returns its binary representation.
func EncodeInt64(x int64) []byte {
	var buf [8]byte

	binary.BigEndian.PutUint64(buf[:], uint64(x)+math.MaxInt64+1)
	return buf[:]
}

// DecodeInt64 takes a byte slice and decodes it into an int64.
func DecodeInt64(buf []byte) (int64, error) {
	x, err := DecodeUint64(buf)
	x -= math.MaxInt64 + 1
	return int64(x), err
}

// EncodeFloat32 takes an float32 and returns its binary representation.
func EncodeFloat32(x float32) []byte {
	fb := math.Float32bits(x)
	if x >= 0 {
		fb ^= 1 << 31
	} else {
		fb ^= 1<<32 - 1
	}
	return EncodeUint32(fb)
}

// DecodeFloat32 takes a byte slice and decodes it into an float32.
func DecodeFloat32(buf []byte) (float32, error) {
	x := binary.BigEndian.Uint32(buf)

	if (x & (1 << 31)) != 0 {
		x ^= 1 << 31
	} else {
		x ^= 1<<32 - 1
	}
	return math.Float32frombits(x), nil
}

// EncodeFloat64 takes an float64 and returns its binary representation.
func EncodeFloat64(x float64) []byte {
	fb := math.Float64bits(x)
	if x >= 0 {
		fb ^= 1 << 63
	} else {
		fb ^= 1<<64 - 1
	}
	return EncodeUint64(fb)
}

// DecodeFloat64 takes a byte slice and decodes it into an float64.
func DecodeFloat64(buf []byte) (float64, error) {
	x := binary.BigEndian.Uint64(buf)

	if (x & (1 << 63)) != 0 {
		x ^= 1 << 63
	} else {
		x ^= 1<<64 - 1
	}
	return math.Float64frombits(x), nil
}

// ZeroValue returns a field whose value is equal to the Go zero value
// of the selected type.
func ZeroValue(t Type) Field {
	switch t {
	case Bytes:
		return NewBytes("", nil)
	case String:
		return NewString("", "")
	case Bool:
		return NewBool("", false)
	case Uint:
		return NewUint("", 0)
	case Uint8:
		return NewUint8("", 0)
	case Uint16:
		return NewUint16("", 0)
	case Uint32:
		return NewUint32("", 0)
	case Uint64:
		return NewUint64("", 0)
	case Int:
		return NewInt("", 0)
	case Int8:
		return NewInt8("", 0)
	case Int16:
		return NewInt16("", 0)
	case Int32:
		return NewInt32("", 0)
	case Int64:
		return NewInt64("", 0)
	case Float32:
		return NewFloat32("", 0)
	case Float64:
		return NewFloat64("", 0)
	}

	return Field{}
}

var (
	bytesZeroValue   = ZeroValue(Bytes)
	stringZeroValue  = ZeroValue(String)
	boolZeroValue    = ZeroValue(Bool)
	uintZeroValue    = ZeroValue(Uint)
	uint8ZeroValue   = ZeroValue(Uint8)
	uint16ZeroValue  = ZeroValue(Uint16)
	uint32ZeroValue  = ZeroValue(Uint32)
	uint64ZeroValue  = ZeroValue(Uint64)
	intZeroValue     = ZeroValue(Int)
	int8ZeroValue    = ZeroValue(Int8)
	int16ZeroValue   = ZeroValue(Int16)
	int32ZeroValue   = ZeroValue(Int32)
	int64ZeroValue   = ZeroValue(Int64)
	float32ZeroValue = ZeroValue(Float32)
	float64ZeroValue = ZeroValue(Float64)
)

// IsZeroValue indicates if the field data is the zero value for the field type.
// This function doesn't perform any allocation.
func IsZeroValue(t Type, data []byte) bool {
	switch t {
	case Bytes:
		return bytes.Equal(data, bytesZeroValue.Data)
	case String:
		return bytes.Equal(data, stringZeroValue.Data)
	case Bool:
		return bytes.Equal(data, boolZeroValue.Data)
	case Uint:
		return bytes.Equal(data, uintZeroValue.Data)
	case Uint8:
		return bytes.Equal(data, uint8ZeroValue.Data)
	case Uint16:
		return bytes.Equal(data, uint16ZeroValue.Data)
	case Uint32:
		return bytes.Equal(data, uint32ZeroValue.Data)
	case Uint64:
		return bytes.Equal(data, uint64ZeroValue.Data)
	case Int:
		return bytes.Equal(data, intZeroValue.Data)
	case Int8:
		return bytes.Equal(data, int8ZeroValue.Data)
	case Int16:
		return bytes.Equal(data, int16ZeroValue.Data)
	case Int32:
		return bytes.Equal(data, int32ZeroValue.Data)
	case Int64:
		return bytes.Equal(data, int64ZeroValue.Data)
	case Float32:
		return bytes.Equal(data, float32ZeroValue.Data)
	case Float64:
		return bytes.Equal(data, float64ZeroValue.Data)
	}

	return false
}

// Decode a field based on its type and returns its Go value.
func Decode(f Field) (interface{}, error) {
	switch f.Type {
	case Bytes:
		return DecodeBytes(f.Data)
	case String:
		return DecodeString(f.Data)
	case Bool:
		return DecodeBool(f.Data)
	case Uint:
		return DecodeUint(f.Data)
	case Uint8:
		return DecodeUint8(f.Data)
	case Uint16:
		return DecodeUint16(f.Data)
	case Uint32:
		return DecodeUint32(f.Data)
	case Uint64:
		return DecodeUint64(f.Data)
	case Int:
		return DecodeInt(f.Data)
	case Int8:
		return DecodeInt8(f.Data)
	case Int16:
		return DecodeInt16(f.Data)
	case Int32:
		return DecodeInt32(f.Data)
	case Int64:
		return DecodeInt64(f.Data)
	case Float32:
		return DecodeFloat32(f.Data)
	case Float64:
		return DecodeFloat64(f.Data)
	}

	return nil, errors.New("unknown type")
}
