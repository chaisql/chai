package field

import (
	"encoding/binary"
	"errors"
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

// A Field is a typed information stored in the database.
type Field struct {
	Name string
	Type Type
	Data []byte
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

// EncodeString takes a string and returns its binary representation.
func EncodeString(x string) []byte {
	return []byte(x)
}

// EncodeBool takes a bool and returns its binary representation.
func EncodeBool(x bool) []byte {
	if x {
		return []byte{1}
	}
	return []byte{0}
}

// EncodeUint takes an uint and returns its binary representation.
func EncodeUint(x uint) []byte {
	return EncodeUint64(uint64(x))
}

// EncodeUint8 takes an uint8 and returns its binary representation.
func EncodeUint8(x uint8) []byte {
	return []byte{x}
}

// EncodeUint16 takes an uint16 and returns its binary representation.
func EncodeUint16(x uint16) []byte {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], x)
	return buf[:]
}

// EncodeUint32 takes an uint32 and returns its binary representation.
func EncodeUint32(x uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], x)
	return buf[:]
}

// EncodeUint64 takes an uint64 and returns its binary representation.
func EncodeUint64(x uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], x)
	return buf[:]
}

// EncodeInt takes an int and returns its binary representation.
func EncodeInt(x int) []byte {
	return EncodeInt64(int64(x))
}

// EncodeInt8 takes an int8 and returns its binary representation.
func EncodeInt8(x int8) []byte {
	return EncodeUint8(uint8(x))
}

// EncodeInt16 takes an int16 and returns its binary representation.
func EncodeInt16(x int16) []byte {
	return EncodeUint16(uint16(x))
}

// EncodeInt32 takes an int32 and returns its binary representation.
func EncodeInt32(x int32) []byte {
	return EncodeUint32(uint32(x))
}

// EncodeInt64 takes an int64 and returns its binary representation.
func EncodeInt64(x int64) []byte {
	return EncodeUint64(uint64(x))
}

// EncodeFloat32 takes an float32 and returns its binary representation.
func EncodeFloat32(x float32) []byte {
	return EncodeUint32(math.Float32bits(x))
}

// EncodeFloat64 takes an float64 and returns its binary representation.
func EncodeFloat64(x float64) []byte {
	return EncodeUint64(math.Float64bits(x))
}

// DecodeBytes takes a byte slice and returns.
// It is present to ease code generation.
func DecodeBytes(buf []byte) ([]byte, error) {
	return buf, nil
}

// DecodeString takes a byte slice and decodes it into a string.
func DecodeString(buf []byte) (string, error) {
	return string(buf), nil
}

// DecodeBool takes a byte slice and decodes it into a boolean.
func DecodeBool(buf []byte) (bool, error) {
	if len(buf) != 1 {
		return false, errors.New("cannot decode buffer to bool")
	}
	return buf[0] == 1, nil
}

// DecodeUint takes a byte slice and decodes it into a uint.
func DecodeUint(buf []byte) (uint, error) {
	x, err := DecodeUint64(buf)
	return uint(x), err
}

// DecodeUint8 takes a byte slice and decodes it into a uint8.
func DecodeUint8(buf []byte) (uint8, error) {
	if len(buf) == 0 {
		return 0, errors.New("cannot decode buffer to uint8")
	}

	return buf[0], nil
}

// DecodeUint16 takes a byte slice and decodes it into a uint16.
func DecodeUint16(buf []byte) (uint16, error) {
	if len(buf) < 2 {
		return 0, errors.New("cannot decode buffer to uint16")
	}

	return binary.BigEndian.Uint16(buf), nil
}

// DecodeUint32 takes a byte slice and decodes it into a uint32.
func DecodeUint32(buf []byte) (uint32, error) {
	if len(buf) < 4 {
		return 0, errors.New("cannot decode buffer to uint32")
	}

	return binary.BigEndian.Uint32(buf), nil
}

// DecodeUint64 takes a byte slice and decodes it into a uint64.
func DecodeUint64(buf []byte) (uint64, error) {
	if len(buf) < 8 {
		return 0, errors.New("cannot decode buffer to uint64")
	}

	return binary.BigEndian.Uint64(buf), nil
}

// DecodeInt takes a byte slice and decodes it into an int.
func DecodeInt(buf []byte) (int, error) {
	x, err := DecodeInt64(buf)
	return int(x), err
}

// DecodeInt8 takes a byte slice and decodes it into an int8.
func DecodeInt8(buf []byte) (int8, error) {
	v, err := DecodeUint8(buf)
	if err != nil {
		return 0, err
	}

	return int8(v), nil
}

// DecodeInt16 takes a byte slice and decodes it into an int16.
func DecodeInt16(buf []byte) (int16, error) {
	v, err := DecodeUint16(buf)
	if err != nil {
		return 0, err
	}

	return int16(v), nil
}

// DecodeInt32 takes a byte slice and decodes it into an int32.
func DecodeInt32(buf []byte) (int32, error) {
	v, err := DecodeUint32(buf)
	if err != nil {
		return 0, err
	}

	return int32(v), nil
}

// DecodeInt64 takes a byte slice and decodes it into an int64.
func DecodeInt64(buf []byte) (int64, error) {
	v, err := DecodeUint64(buf)
	if err != nil {
		return 0, err
	}

	return int64(v), nil
}

// DecodeFloat32 takes a byte slice and decodes it into an float32.
func DecodeFloat32(buf []byte) (float32, error) {
	x, err := DecodeUint32(buf)
	if err != nil {
		return 0, err
	}

	return math.Float32frombits(x), nil
}

// DecodeFloat64 takes a byte slice and decodes it into an float64.
func DecodeFloat64(buf []byte) (float64, error) {
	x, err := DecodeUint64(buf)
	if err != nil {
		return 0, err
	}

	return math.Float64frombits(x), nil
}
