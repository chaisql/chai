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

// EncodeValue encodes a value as a key.
// It only works with scalars and doesn't support documents and arrays.
func EncodeValue(v document.Value) ([]byte, error) {
	switch v.Type {
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

	return nil, errors.New("cannot encode type " + v.Type.String() + " as key")
}

// DecodeValue takes some encoded data and decodes it to the target type t.
func DecodeValue(t document.ValueType, data []byte) (document.Value, error) {
	switch t {
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
