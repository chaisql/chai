// Package encoding provides types and functions to encode and decode documents and values.
//
// Encoding values
//
// Each type is encoded in a way that allows ordering to be preserved. That way, if vA < vB,
// where vA and vB are two unencoded values of the same type, then eA < eB, where eA and eB
// are the respective encoded values of vA and vB.
package encoding

import (
	"encoding/binary"
	"errors"
	"math"
	"time"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding/msgpack"
)

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

func EncodeValue(v document.Value) ([]byte, error) {
	switch v.Type {
	case document.DocumentValue:
		return msgpack.EncodeDocument(v.V.(document.Document))
	case document.ArrayValue:
		return msgpack.EncodeArray(v.V.(document.Array))
	case document.BlobValue:
		return v.V.([]byte), nil
	case document.TextValue:
		return EncodeString(v.V.(string)), nil
	case document.BoolValue:
		return EncodeBool(v.V.(bool)), nil
	case document.IntegerValue:
		return EncodeInt64(v.V.(int64)), nil
	case document.DoubleValue:
		return EncodeFloat64(v.V.(float64)), nil
	case document.DurationValue:
		return EncodeInt64(int64(v.V.(time.Duration))), nil
	case document.NullValue:
		return nil, nil
	}

	return nil, errors.New("unknown type")
}

// DecodeValue takes some encoded data and decodes it to the target type t.
func DecodeValue(t document.ValueType, data []byte) (document.Value, error) {
	switch t {
	case document.DocumentValue:
		return document.NewDocumentValue(msgpack.EncodedDocument(data)), nil
	case document.ArrayValue:
		return document.NewArrayValue(msgpack.EncodedArray(data)), nil
	case document.BlobValue:
		return document.NewBlobValue(data), nil
	case document.TextValue:
		x, err := DecodeString(data)
		if err != nil {
			return document.Value{}, err
		}
		return document.NewTextValue(x), nil
	case document.BoolValue:
		x, err := DecodeBool(data)
		if err != nil {
			return document.Value{}, err
		}
		return document.NewBoolValue(x), nil
	case document.IntegerValue:
		x, err := DecodeInt64(data)
		if err != nil {
			return document.Value{}, err
		}
		return document.NewIntegerValue(x), nil
	case document.DoubleValue:
		x, err := DecodeFloat64(data)
		if err != nil {
			return document.Value{}, err
		}
		return document.NewDoubleValue(x), nil
	case document.DurationValue:
		x, err := DecodeInt64(data)
		if err != nil {
			return document.Value{}, err
		}
		return document.NewDurationValue(time.Duration(x)), nil
	case document.NullValue:
		return document.NewNullValue(), nil
	}

	return document.Value{}, errors.New("unknown type")
}
