// Package value defines types to manipulate and encode values.
package value

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// Type represents a value type supported by the database.
type Type uint8

// List of supported value types.
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
	Float64

	Null
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
	case Float64:
		return "Float64"
	case Null:
		return "Null"
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
	case "float64":
		return Float64
	case "nil":
		return Null
	}

	return 0
}

// A Value stores encoded data alongside its type.
type Value struct {
	Type Type
	Data []byte
	v    interface{}
}

// New creates a value whose type is infered from x.
func New(x interface{}) (Value, error) {
	switch v := x.(type) {
	case []byte:
		return NewBytes(v), nil
	case string:
		return NewString(v), nil
	case bool:
		return NewBool(v), nil
	case uint:
		return NewUint(v), nil
	case uint8:
		return NewUint8(v), nil
	case uint16:
		return NewUint16(v), nil
	case uint32:
		return NewUint32(v), nil
	case uint64:
		return NewUint64(v), nil
	case int:
		return NewInt(v), nil
	case int8:
		return NewInt8(v), nil
	case int16:
		return NewInt16(v), nil
	case int32:
		return NewInt32(v), nil
	case int64:
		return NewInt64(v), nil
	case float64:
		return NewFloat64(v), nil
	case nil:
		return NewNull(), nil
	default:
		return Value{}, fmt.Errorf("unsupported type %T", x)
	}
}

// NewBytes encodes x and returns a value.
func NewBytes(x []byte) Value {
	return Value{
		Type: Bytes,
		Data: x,
	}
}

// NewString encodes x and returns a value.
func NewString(x string) Value {
	return Value{
		Type: String,
		Data: []byte(x),
	}
}

// NewBool encodes x and returns a value.
func NewBool(x bool) Value {
	return Value{
		Type: Bool,
		Data: EncodeBool(x),
	}
}

// NewUint encodes x and returns a value.
func NewUint(x uint) Value {
	return Value{
		Type: Uint,
		Data: EncodeUint(x),
	}
}

// NewUint8 encodes x and returns a value.
func NewUint8(x uint8) Value {
	return Value{
		Type: Uint8,
		Data: EncodeUint8(x),
	}
}

// NewUint16 encodes x and returns a value.
func NewUint16(x uint16) Value {
	return Value{
		Type: Uint16,
		Data: EncodeUint16(x),
	}
}

// NewUint32 encodes x and returns a value.
func NewUint32(x uint32) Value {
	return Value{
		Type: Uint32,
		Data: EncodeUint32(x),
	}
}

// NewUint64 encodes x and returns a value.
func NewUint64(x uint64) Value {
	return Value{
		Type: Uint64,
		Data: EncodeUint64(x),
	}
}

// NewInt encodes x and returns a value.
func NewInt(x int) Value {
	return Value{
		Type: Int,
		Data: EncodeInt(x),
	}
}

// NewInt8 encodes x and returns a value.
func NewInt8(x int8) Value {
	return Value{
		Type: Int8,
		Data: EncodeInt8(x),
	}
}

// NewInt16 encodes x and returns a value.
func NewInt16(x int16) Value {
	return Value{
		Type: Int16,
		Data: EncodeInt16(x),
	}
}

// NewInt32 encodes x and returns a value.
func NewInt32(x int32) Value {
	return Value{
		Type: Int32,
		Data: EncodeInt32(x),
	}
}

// NewInt64 encodes x and returns a value.
func NewInt64(x int64) Value {
	return Value{
		Type: Int64,
		Data: EncodeInt64(x),
	}
}

// NewFloat64 encodes x and returns a value.
func NewFloat64(x float64) Value {
	return Value{
		Type: Float64,
		Data: EncodeFloat64(x),
	}
}

// NewNull returns a Null value.
func NewNull() Value {
	return Value{
		Type: Null,
	}
}

func (v *Value) decode() error {
	var err error

	switch v.Type {
	case Bytes:
		v.v, err = DecodeBytes(v.Data)
	case String:
		v.v, err = DecodeString(v.Data)
	case Bool:
		v.v, err = DecodeBool(v.Data)
	case Uint:
		v.v, err = DecodeUint(v.Data)
	case Uint8:
		v.v, err = DecodeUint8(v.Data)
	case Uint16:
		v.v, err = DecodeUint16(v.Data)
	case Uint32:
		v.v, err = DecodeUint32(v.Data)
	case Uint64:
		v.v, err = DecodeUint64(v.Data)
	case Int:
		v.v, err = DecodeInt(v.Data)
	case Int8:
		v.v, err = DecodeInt8(v.Data)
	case Int16:
		v.v, err = DecodeInt16(v.Data)
	case Int32:
		v.v, err = DecodeInt32(v.Data)
	case Int64:
		v.v, err = DecodeInt64(v.Data)
	case Float64:
		v.v, err = DecodeFloat64(v.Data)
	case Null:
		v.v = nil
	default:
		return errors.New("unknown type")
	}

	return err
}

// Decode a value based on its type, caches it and returns its Go value.
// If the decoded value is already cached, returns it immediatly.
func (v Value) Decode() (interface{}, error) {
	if v.v == nil {
		err := v.decode()
		if err != nil {
			return nil, err
		}
	}

	return v.v, nil
}

// String returns a string representation of the value. It implements the fmt.Stringer interface.
func (v Value) String() string {
	var vv interface{}

	switch v.Type {
	case Bytes:
		vv, _ = DecodeBytes(v.Data)
	case String:
		vv, _ = DecodeString(v.Data)
	case Bool:
		vv, _ = DecodeBool(v.Data)
	case Uint:
		vv, _ = DecodeUint(v.Data)
	case Uint8:
		vv, _ = DecodeUint8(v.Data)
	case Uint16:
		vv, _ = DecodeUint16(v.Data)
	case Uint32:
		vv, _ = DecodeUint32(v.Data)
	case Uint64:
		vv, _ = DecodeUint64(v.Data)
	case Int:
		vv, _ = DecodeInt(v.Data)
	case Int8:
		vv, _ = DecodeInt8(v.Data)
	case Int16:
		vv, _ = DecodeInt16(v.Data)
	case Int32:
		vv, _ = DecodeInt32(v.Data)
	case Int64:
		vv, _ = DecodeInt64(v.Data)
	case Float64:
		vv, _ = DecodeFloat64(v.Data)
	}

	return fmt.Sprintf("%v", vv)
}

// DecodeTo decodes v to the selected type when possible.
func (v Value) DecodeTo(t Type) (Value, error) {
	if v.Type == t {
		return v, nil
	}

	switch t {
	case Bytes:
		x, err := v.DecodeToBytes()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Bytes,
			Data: EncodeBytes(x),
			v:    x,
		}, nil
	case String:
		x, err := v.DecodeToString()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: String,
			Data: EncodeString(x),
			v:    x,
		}, nil
	case Bool:
		x, err := v.DecodeToBool()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Bool,
			Data: EncodeBool(x),
			v:    x,
		}, nil
	case Uint:
		x, err := v.DecodeToUint()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Uint,
			Data: EncodeUint(x),
			v:    x,
		}, nil
	case Uint8:
		x, err := v.DecodeToUint8()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Uint8,
			Data: EncodeUint8(x),
			v:    x,
		}, nil
	case Uint16:
		x, err := v.DecodeToUint16()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Uint16,
			Data: EncodeUint16(x),
			v:    x,
		}, nil
	case Uint32:
		x, err := v.DecodeToUint32()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Uint32,
			Data: EncodeUint32(x),
			v:    x,
		}, nil
	case Uint64:
		x, err := v.DecodeToUint64()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Uint64,
			Data: EncodeUint64(x),
			v:    x,
		}, nil
	case Int:
		x, err := v.DecodeToInt()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Int,
			Data: EncodeInt(x),
			v:    x,
		}, nil
	case Int8:
		x, err := v.DecodeToInt8()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Int8,
			Data: EncodeInt8(x),
			v:    x,
		}, nil
	case Int16:
		x, err := v.DecodeToInt16()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Int16,
			Data: EncodeInt16(x),
			v:    x,
		}, nil
	case Int32:
		x, err := v.DecodeToInt32()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Int32,
			Data: EncodeInt32(x),
			v:    x,
		}, nil
	case Int64:
		x, err := v.DecodeToInt64()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Int64,
			Data: EncodeInt64(x),
			v:    x,
		}, nil
	case Float64:
		x, err := v.DecodeToFloat64()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Float64,
			Data: EncodeFloat64(x),
			v:    x,
		}, nil
	}

	return Value{}, fmt.Errorf("can't convert %q to %q", v.Type, t)
}

// DecodeToBytes returns v.Data. It's a convenience method to ease code generation.
func (v Value) DecodeToBytes() ([]byte, error) {
	return v.Data, nil
}

// DecodeToString turns a value of type String or Bytes into a string.
// If fails if it's used with any other type.
func (v Value) DecodeToString() (string, error) {
	if v.Type == String {
		return DecodeString(v.Data)
	}

	if v.Type == Bytes {
		return string(v.Data), nil
	}

	return "", fmt.Errorf("can't convert %q to string", v.Type)
}

// DecodeToBool returns true if v is truthy, otherwise it returns false.
func (v Value) DecodeToBool() (bool, error) {
	if v.Type == Bool {
		return DecodeBool(v.Data)
	}

	return !IsZeroValue(v.Type, v.Data), nil
}

// DecodeToUint turns any number into a uint.
// It doesn't work with other types.
func (v Value) DecodeToUint() (uint, error) {
	if v.Type == Uint {
		return DecodeUint(v.Data)
	}

	if IsNumber(v.Type) {
		x, err := decodeAsInt64(v)
		if err != nil {
			return 0, err
		}
		return uint(x), nil
	}

	return 0, fmt.Errorf("can't convert %q to uint", v.Type)
}

// DecodeToUint8 turns any number into a uint8.
// It doesn't work with other types.
func (v Value) DecodeToUint8() (uint8, error) {
	if v.Type == Uint8 {
		return DecodeUint8(v.Data)
	}

	if IsNumber(v.Type) {
		x, err := decodeAsInt64(v)
		if err != nil {
			return 0, err
		}
		return uint8(x), nil
	}

	return 0, fmt.Errorf("can't convert %q to uint8", v.Type)
}

// DecodeToUint16 turns any number into a uint16.
// It doesn't work with other types.
func (v Value) DecodeToUint16() (uint16, error) {
	if v.Type == Uint16 {
		return DecodeUint16(v.Data)
	}

	if IsNumber(v.Type) {
		x, err := decodeAsInt64(v)
		if err != nil {
			return 0, err
		}
		return uint16(x), nil
	}

	return 0, fmt.Errorf("can't convert %q to uint16", v.Type)
}

// DecodeToUint32 turns any number into a uint32.
// It doesn't work with other types.
func (v Value) DecodeToUint32() (uint32, error) {
	if v.Type == Uint32 {
		return DecodeUint32(v.Data)
	}

	if IsNumber(v.Type) {
		x, err := decodeAsInt64(v)
		if err != nil {
			return 0, err
		}
		return uint32(x), nil
	}

	return 0, fmt.Errorf("can't convert %q to uint32", v.Type)
}

// DecodeToUint64 turns any number into a uint64.
// It doesn't work with other types.
func (v Value) DecodeToUint64() (uint64, error) {
	if v.Type == Uint64 {
		return DecodeUint64(v.Data)
	}

	if IsNumber(v.Type) {
		x, err := decodeAsInt64(v)
		if err != nil {
			return 0, err
		}
		return uint64(x), nil
	}

	return 0, fmt.Errorf("can't convert %q to uint64", v.Type)
}

// DecodeToInt turns any number into an int.
// It doesn't work with other types.
func (v Value) DecodeToInt() (int, error) {
	if v.Type == Int {
		return DecodeInt(v.Data)
	}

	if IsNumber(v.Type) {
		x, err := decodeAsInt64(v)
		if err != nil {
			return 0, err
		}
		return int(x), nil
	}

	return 0, fmt.Errorf("can't convert %q to Int", v.Type)
}

// DecodeToInt8 turns any number into an int8.
// It doesn't work with other types.
func (v Value) DecodeToInt8() (int8, error) {
	if v.Type == Int8 {
		return DecodeInt8(v.Data)
	}

	if IsNumber(v.Type) {
		x, err := decodeAsInt64(v)
		if err != nil {
			return 0, err
		}
		return int8(x), nil
	}

	return 0, fmt.Errorf("can't convert %q to Int8", v.Type)
}

// DecodeToInt16 turns any number into an int16.
// It doesn't work with other types.
func (v Value) DecodeToInt16() (int16, error) {
	if v.Type == Int16 {
		return DecodeInt16(v.Data)
	}

	if IsNumber(v.Type) {
		x, err := decodeAsInt64(v)
		if err != nil {
			return 0, err
		}
		return int16(x), nil
	}

	return 0, fmt.Errorf("can't convert %q to int16", v.Type)
}

// DecodeToInt32 turns any number into an int32.
// It doesn't work with other types.
func (v Value) DecodeToInt32() (int32, error) {
	if v.Type == Int32 {
		return DecodeInt32(v.Data)
	}

	if IsNumber(v.Type) {
		x, err := decodeAsInt64(v)
		if err != nil {
			return 0, err
		}
		return int32(x), nil
	}

	return 0, fmt.Errorf("can't convert %q to int32", v.Type)
}

// DecodeToInt64 turns any number into an int64.
// It doesn't work with other types.
func (v Value) DecodeToInt64() (int64, error) {
	if v.Type == Int64 {
		return DecodeInt64(v.Data)
	}

	if IsNumber(v.Type) {
		return decodeAsInt64(v)
	}

	return 0, fmt.Errorf("can't convert %q to int64", v.Type)
}

// DecodeToFloat64 turns any number into a float64.
// It doesn't work with other types.
func (v Value) DecodeToFloat64() (float64, error) {
	if v.Type == Float64 {
		return DecodeFloat64(v.Data)
	}

	if IsInteger(v.Type) {
		x, err := decodeAsInt64(v)
		if err != nil {
			return 0, err
		}
		return float64(x), nil
	}

	return 0, fmt.Errorf("can't convert %q to float64", v.Type)
}

func decodeAsInt64(v Value) (int64, error) {
	var i int64

	switch v.Type {
	case Uint:
		x, err := DecodeUint(v.Data)
		if err != nil {
			return 0, err
		}
		i = int64(x)
	case Uint8:
		x, err := DecodeUint8(v.Data)
		if err != nil {
			return 0, err
		}
		i = int64(x)
	case Uint16:
		x, err := DecodeUint16(v.Data)
		if err != nil {
			return 0, err
		}
		i = int64(x)
	case Uint32:
		x, err := DecodeUint32(v.Data)
		if err != nil {
			return 0, err
		}
		i = int64(x)
	case Uint64:
		x, err := DecodeUint64(v.Data)
		if err != nil {
			return 0, err
		}
		i = int64(x)
	case Int:
		x, err := DecodeInt(v.Data)
		if err != nil {
			return 0, err
		}
		i = int64(x)
	case Int8:
		x, err := DecodeInt8(v.Data)
		if err != nil {
			return 0, err
		}
		i = int64(x)
	case Int16:
		x, err := DecodeInt16(v.Data)
		if err != nil {
			return 0, err
		}
		i = int64(x)
	case Int32:
		x, err := DecodeInt32(v.Data)
		if err != nil {
			return 0, err
		}
		i = int64(x)
	case Int64:
		return DecodeInt64(v.Data)
	case Float64:
		x, err := DecodeFloat64(v.Data)
		if err != nil {
			return 0, err
		}
		i = int64(x)
	}

	return i, nil
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

// ZeroValue returns a value whose value is equal to the Go zero value
// of the selected type.
func ZeroValue(t Type) Value {
	switch t {
	case Bytes:
		return NewBytes(nil)
	case String:
		return NewString("")
	case Bool:
		return NewBool(false)
	case Uint:
		return NewUint(0)
	case Uint8:
		return NewUint8(0)
	case Uint16:
		return NewUint16(0)
	case Uint32:
		return NewUint32(0)
	case Uint64:
		return NewUint64(0)
	case Int:
		return NewInt(0)
	case Int8:
		return NewInt8(0)
	case Int16:
		return NewInt16(0)
	case Int32:
		return NewInt32(0)
	case Int64:
		return NewInt64(0)
	case Float64:
		return NewFloat64(0)
	}

	return Value{}
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
	float64ZeroValue = ZeroValue(Float64)
)

// IsZeroValue indicates if the value data is the zero value for the value type.
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
	case Float64:
		return bytes.Equal(data, float64ZeroValue.Data)
	}

	return false
}

// IsNumber returns true if t is either an integer of a float.
func IsNumber(t Type) bool {
	return IsInteger(t) || IsFloat(t)
}

// IsInteger returns true if t is a signed or unsigned integer of any size.
func IsInteger(t Type) bool {
	return t >= Uint && t <= Int64
}

// IsFloat returns true if t is either a Float32 or Float64.
func IsFloat(t Type) bool {
	return t == Float64
}
