// Package key provides types and functions to encode and decode keys.
//
// Encoding keys
//
// Each type is encoded in a way that allows ordering to be preserved. That way, if vA < vB,
// where vA and vB are two unencoded values of the same type, then eA < eB, where eA and eB
// are the respective encoded values of vA and vB.
package key

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"math"
	"time"

	"github.com/genjidb/genji/document"
)

const base64encoder = "-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz"

var base64Encoding = base64.NewEncoding(base64encoder).WithPadding(base64.NoPadding)

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

// AppendNumber takes a number value, integer or double, and encodes it in 16 bytes
// so that encoded integers and doubles are naturally ordered.
// Integers will fist be encoded using AppendInt64 on 8 bytes, then 8 zero-bytes will be
// appended to them.
// Doubles will first be converted to integer, encoded using AppendInt64,
// then AppendFloat64 will be called with the float value.
func AppendNumber(buf []byte, v document.Value) ([]byte, error) {
	if !v.Type.IsNumber() {
		return nil, errors.New("expected number type")
	}

	if v.Type == document.IntegerValue {
		// appending 8 zero bytes so that the integer has the same size as the double
		// but always lower for the same value.
		return append(AppendInt64(buf, v.V.(int64)), 0, 0, 0, 0, 0, 0, 0, 0), nil
	}

	x := v.V.(float64)
	if x > math.MaxInt64 {
		return AppendFloat64(AppendInt64(buf, math.MaxInt64), x), nil
	}
	return AppendFloat64(AppendInt64(buf, int64(x)), x), nil
}

// AppendValue encodes a value as a key.
// It only works with scalars and doesn't support documents and arrays.
func AppendValue(buf []byte, v document.Value) ([]byte, error) {
	if v.Type == document.IntegerValue || v.Type == document.DoubleValue {
		buf = append(buf, byte(document.DoubleValue))
	} else {
		buf = append(buf, byte(v.Type))
	}

	switch v.Type {
	case document.BlobValue:
		return append(buf, v.V.([]byte)...), nil
	case document.TextValue:
		text := v.V.(string)
		b := bytes.NewBuffer(buf)
		enc := base64.NewEncoder(base64Encoding, b)
		_, err := enc.Write([]byte(text))
		if err != nil {
			return nil, err
		}
		err = enc.Close()
		if err != nil {
			return nil, err
		}

		return b.Bytes(), nil
	case document.BoolValue:
		return AppendBool(buf, v.V.(bool)), nil
	case document.IntegerValue, document.DoubleValue:
		return AppendNumber(buf, v)
	case document.DurationValue:
		return AppendInt64(buf, int64(v.V.(time.Duration))), nil
	case document.NullValue:
		return buf, nil
	}

	return nil, errors.New("cannot encode type " + v.Type.String() + " as key")
}

// DecodeValue takes some encoded data and decodes it to the target type t.
func DecodeValue(data []byte) (document.Value, error) {
	t := document.ValueType(data[0])
	data = data[1:]

	switch t {
	case document.BlobValue:
		return document.NewBlobValue(data), nil
	case document.TextValue:
		var buf bytes.Buffer
		dec := base64.NewDecoder(base64Encoding, bytes.NewReader(data))
		_, err := buf.ReadFrom(dec)
		if err != nil {
			return document.Value{}, err
		}
		return document.NewTextValue(buf.String()), nil
	case document.BoolValue:
		return document.NewBoolValue(DecodeBool(data)), nil
	case document.DoubleValue:
		if bytes.Equal(data[8:], []byte{0, 0, 0, 0, 0, 0, 0, 0}) {
			x, err := DecodeInt64(data[:8])
			if err != nil {
				return document.Value{}, err
			}
			return document.NewIntegerValue(x), nil
		}
		x, err := DecodeFloat64(data[8:])
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

// Append encodes a value of the type t as a key.
// The encoded key doesn't include type information.
func Append(buf []byte, t document.ValueType, v interface{}) ([]byte, error) {
	switch t {
	case document.BlobValue:
		return append(buf, v.([]byte)...), nil
	case document.TextValue:
		return append(buf, v.(string)...), nil
	case document.BoolValue:
		return AppendBool(buf, v.(bool)), nil
	case document.IntegerValue:
		return AppendInt64(buf, v.(int64)), nil
	case document.DoubleValue:
		return AppendFloat64(buf, v.(float64)), nil
	case document.DurationValue:
		return AppendInt64(buf, int64(v.(time.Duration))), nil
	case document.NullValue:
		return buf, nil
	}

	return nil, errors.New("cannot encode type " + t.String() + " as key")
}

// Decode takes some encoded data and decodes it to the target type t.
func Decode(t document.ValueType, data []byte) (document.Value, error) {
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
