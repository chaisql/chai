// Package key provides types and functions to encode and decode keys.
//
// Encoding keys
//
// Each type is encoded in a way that allows ordering to be preserved. That way, if vA < vB,
// where vA and vB are two unencoded values of the same type, then eA < eB, where eA and eB
// are the respective encoded values of vA and vB.
package key

import (
	"encoding/binary"
	"errors"
	"math"
	"time"

	"github.com/genjidb/genji/document"
)

// EncodeString takes a string and returns its binary representation.
func EncodeString(buf []byte, x string) {
	copy(buf, x)
}

// AppendBool takes a bool and returns its binary representation.
func AppendBool(buf []byte, x bool) []byte {
	if x {
		return append(buf, 1)
	}
	return append(buf, 0)
}

// DecodeBool takes a byte slice and decodes it into a boolean.
func DecodeBool(buf []byte) bool {
	return buf[0] == 1
}

// AppendUint64 takes an uint64 and returns its binary representation.
func AppendUint64(buf []byte, x uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], x)
	return append(buf, b[:]...)
}

// DecodeUint64 takes a byte slice and decodes it into a uint64.
func DecodeUint64(buf []byte) (uint64, error) {
	if len(buf) < 8 {
		return 0, errors.New("cannot decode buffer to uint64")
	}

	return binary.BigEndian.Uint64(buf), nil
}

// AppendInt64 takes an int64 and returns its binary representation.
func AppendInt64(buf []byte, x int64) []byte {
	var b [8]byte

	binary.BigEndian.PutUint64(b[:], uint64(x)+math.MaxInt64+1)
	return append(buf, b[:]...)
}

// DecodeInt64 takes a byte slice and decodes it into an int64.
func DecodeInt64(buf []byte) (int64, error) {
	x, err := DecodeUint64(buf)
	x -= math.MaxInt64 + 1
	return int64(x), err
}

// AppendFloat64 takes an float64 and returns its binary representation.
func AppendFloat64(buf []byte, x float64) []byte {
	fb := math.Float64bits(x)
	if x >= 0 {
		fb ^= 1 << 63
	} else {
		fb ^= 1<<64 - 1
	}
	return AppendUint64(buf, fb)
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

// AppendValue encodes a value as a key.
// It only works with scalars and doesn't support documents and arrays.
func AppendValue(buf []byte, v document.Value) []byte {
	switch v.Type {
	case document.BlobValue:
		return append(buf, v.V.([]byte)...)
	case document.TextValue:
		return append(buf, v.V.(string)...)
	case document.BoolValue:
		return AppendBool(buf, v.V.(bool))
	case document.IntegerValue:
		return AppendInt64(buf, v.V.(int64))
	case document.DoubleValue:
		return AppendFloat64(buf, v.V.(float64))
	case document.DurationValue:
		return AppendInt64(buf, int64(v.V.(time.Duration)))
	case document.NullValue:
		return buf
	}

	panic("cannot encode type " + v.Type.String() + " as key")
}

// DecodeValue takes some encoded data and decodes it to the target type t.
func DecodeValue(t document.ValueType, data []byte) (document.Value, error) {
	switch t {
	case document.BlobValue:
		return document.NewBlobValue(data), nil
	case document.TextValue:
		return document.NewTextValue(string(data)), nil
	case document.BoolValue:
		return document.NewBoolValue(DecodeBool(data)), nil
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
