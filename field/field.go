package field

import (
	"encoding/binary"
	"errors"
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

func NewString(name string, value string) Field {
	return Field{
		Name: name,
		Type: String,
		Data: []byte(value),
	}
}

func NewBytes(name string, value []byte) Field {
	return Field{
		Name: name,
		Type: Bytes,
		Data: value,
	}
}

func NewBool(name string, value bool) Field {
	return Field{
		Name: name,
		Type: Bool,
		Data: EncodeBool(value),
	}
}

func NewUint(name string, value uint) Field {
	return Field{
		Name: name,
		Type: Uint,
		Data: EncodeUint(value),
	}
}

func NewUint8(name string, value uint8) Field {
	return Field{
		Name: name,
		Type: Uint8,
		Data: EncodeUint8(value),
	}
}

func NewUint16(name string, value uint16) Field {
	return Field{
		Name: name,
		Type: Uint16,
		Data: EncodeUint16(value),
	}
}

func NewUint32(name string, value uint32) Field {
	return Field{
		Name: name,
		Type: Uint32,
		Data: EncodeUint32(value),
	}
}

func NewUint64(name string, value uint64) Field {
	return Field{
		Name: name,
		Type: Uint64,
		Data: EncodeUint64(value),
	}
}

func NewInt(name string, value int) Field {
	return Field{
		Name: name,
		Type: Int,
		Data: EncodeInt(value),
	}
}

func NewInt8(name string, value int8) Field {
	return Field{
		Name: name,
		Type: Int8,
		Data: EncodeInt8(value),
	}
}

func NewInt16(name string, value int16) Field {
	return Field{
		Name: name,
		Type: Int16,
		Data: EncodeInt16(value),
	}
}

func NewInt32(name string, value int32) Field {
	return Field{
		Name: name,
		Type: Int32,
		Data: EncodeInt32(value),
	}
}

func NewInt64(name string, value int64) Field {
	return Field{
		Name: name,
		Type: Int64,
		Data: EncodeInt64(value),
	}
}

func NewFloat32(name string, value float32) Field {
	return Field{
		Name: name,
		Type: Float32,
		Data: EncodeFloat32(value),
	}
}

func NewFloat64(name string, value float64) Field {
	return Field{
		Name: name,
		Type: Float64,
		Data: EncodeFloat64(value),
	}
}

func EncodeBool(v bool) []byte {
	if v {
		return []byte{1}
	}
	return []byte{0}
}

func EncodeUint(v uint) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, uint64(v))
	return buf[:n]
}

func EncodeUint8(v uint8) []byte {
	return []byte{v}
}

func EncodeUint16(v uint16) []byte {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], v)
	return buf[:]
}

func EncodeUint32(v uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)
	return buf[:]
}

func EncodeUint64(v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return buf[:]
}

func EncodeInt(v int) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, int64(v))
	return buf[:n]
}

func EncodeInt8(v int8) []byte {
	return EncodeUint8(uint8(v))
}

func EncodeInt16(v int16) []byte {
	return EncodeUint16(uint16(v))
}

func EncodeInt32(v int32) []byte {
	return EncodeUint32(uint32(v))
}

// EncodeInt64 takes an int64 and returns its binary representation.
func EncodeInt64(v int64) []byte {
	return EncodeUint64(uint64(v))
}

func EncodeFloat32(v float32) []byte {
	return nil
}

func EncodeFloat64(v float64) []byte {
	return nil
}

func DecodeBool(v []byte) (bool, error) {
	return false, nil
}

func DecodeUint(v []byte) (uint, error) {
	return 0, nil
}

func DecodeUint8(v []byte) (uint8, error) {
	return 0, nil
}

func DecodeUint16(v []byte) (uint16, error) {
	return 0, nil
}

func DecodeUint32(v []byte) (uint32, error) {
	return 0, nil
}

func DecodeUint64(v []byte) (uint64, error) {
	return 0, nil
}

func DecodeInt(v []byte) (int, error) {
	return 0, nil
}

func DecodeInt8(v []byte) (int8, error) {
	return 0, nil
}

func DecodeInt16(v []byte) (int16, error) {
	return 0, nil
}

func DecodeInt32(v []byte) (int32, error) {
	return 0, nil
}

// DecodeInt64 takes a byte slice and decodes it into an int64.
// An error is returned if the value is invalid.
func DecodeInt64(v []byte) (int64, error) {
	i, n := binary.Varint(v)
	if n <= 0 {
		return 0, errors.New("field: cannot decode value to in64")
	}

	return i, nil
}

func DecodeFloat32(v []byte) (float32, error) {
	return 0, nil
}

func DecodeFloat64(v []byte) (float64, error) {
	return 0, nil
}
