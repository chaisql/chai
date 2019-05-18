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
	String Type = iota + 1
	Bytes
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

func NewString(name string, x string) Field {
	return Field{
		Name: name,
		Type: String,
		Data: []byte(x),
	}
}

func NewBytes(name string, x []byte) Field {
	return Field{
		Name: name,
		Type: Bytes,
		Data: x,
	}
}

func NewBool(name string, x bool) Field {
	return Field{
		Name: name,
		Type: Bool,
		Data: EncodeBool(x),
	}
}

func NewUint(name string, x uint) Field {
	return Field{
		Name: name,
		Type: Uint,
		Data: EncodeUint(x),
	}
}

func NewUint8(name string, x uint8) Field {
	return Field{
		Name: name,
		Type: Uint8,
		Data: EncodeUint8(x),
	}
}

func NewUint16(name string, x uint16) Field {
	return Field{
		Name: name,
		Type: Uint16,
		Data: EncodeUint16(x),
	}
}

func NewUint32(name string, x uint32) Field {
	return Field{
		Name: name,
		Type: Uint32,
		Data: EncodeUint32(x),
	}
}

func NewUint64(name string, x uint64) Field {
	return Field{
		Name: name,
		Type: Uint64,
		Data: EncodeUint64(x),
	}
}

func NewInt(name string, x int) Field {
	return Field{
		Name: name,
		Type: Int,
		Data: EncodeInt(x),
	}
}

func NewInt8(name string, x int8) Field {
	return Field{
		Name: name,
		Type: Int8,
		Data: EncodeInt8(x),
	}
}

func NewInt16(name string, x int16) Field {
	return Field{
		Name: name,
		Type: Int16,
		Data: EncodeInt16(x),
	}
}

func NewInt32(name string, x int32) Field {
	return Field{
		Name: name,
		Type: Int32,
		Data: EncodeInt32(x),
	}
}

func NewInt64(name string, x int64) Field {
	return Field{
		Name: name,
		Type: Int64,
		Data: EncodeInt64(x),
	}
}

func NewFloat32(name string, x float32) Field {
	return Field{
		Name: name,
		Type: Float32,
		Data: EncodeFloat32(x),
	}
}

func NewFloat64(name string, x float64) Field {
	return Field{
		Name: name,
		Type: Float64,
		Data: EncodeFloat64(x),
	}
}

func EncodeBool(x bool) []byte {
	if x {
		return []byte{1}
	}
	return []byte{0}
}

func EncodeUint(x uint) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, uint64(x))
	return buf[:n]
}

func EncodeUint8(x uint8) []byte {
	return []byte{x}
}

func EncodeUint16(x uint16) []byte {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], x)
	return buf[:]
}

func EncodeUint32(x uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], x)
	return buf[:]
}

func EncodeUint64(x uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], x)
	return buf[:]
}

func EncodeInt(x int) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, int64(x))
	return buf[:n]
}

func EncodeInt8(x int8) []byte {
	return EncodeUint8(uint8(x))
}

func EncodeInt16(x int16) []byte {
	return EncodeUint16(uint16(x))
}

func EncodeInt32(x int32) []byte {
	return EncodeUint32(uint32(x))
}

// EncodeInt64 takes an int64 and returns its binary representation.
func EncodeInt64(x int64) []byte {
	return EncodeUint64(uint64(x))
}

func EncodeFloat32(x float32) []byte {
	return EncodeUint32(math.Float32bits(x))
}

func EncodeFloat64(x float64) []byte {
	return EncodeUint64(math.Float64bits(x))
}

func DecodeBool(buf []byte) (bool, error) {
	if len(buf) != 1 {
		return false, errors.New("cannot decode buffer to bool")
	}
	return buf[0] == 1, nil
}

func DecodeUint(buf []byte) (uint, error) {
	return 0, nil
}

func DecodeUint8(buf []byte) (uint8, error) {
	return 0, nil
}

func DecodeUint16(buf []byte) (uint16, error) {
	return 0, nil
}

func DecodeUint32(buf []byte) (uint32, error) {
	return 0, nil
}

func DecodeUint64(buf []byte) (uint64, error) {
	return 0, nil
}

func DecodeInt(buf []byte) (int, error) {
	return 0, nil
}

func DecodeInt8(buf []byte) (int8, error) {
	return 0, nil
}

func DecodeInt16(buf []byte) (int16, error) {
	return 0, nil
}

func DecodeInt32(buf []byte) (int32, error) {
	return 0, nil
}

// DecodeInt64 takes a byte slice and decodes it into an int64.
// An error is returned if the value is invalid.
func DecodeInt64(buf []byte) (int64, error) {
	i, n := binary.Varint(buf)
	if n <= 0 {
		return 0, errors.New("cannot decode buffer to in64")
	}

	return i, nil
}

func DecodeFloat32(buf []byte) (float32, error) {
	return 0, nil
}

func DecodeFloat64(buf []byte) (float64, error) {
	return 0, nil
}
