// Package binarysort provides types and functions to encode into naturally sorted binary representations.
// That way, if vA < vB, where vA and vB are two unencoded values of the same type, then eA < eB, where eA and eB
// are the respective encoded values of vA and vB.
package binarysort

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"math"

	"github.com/genjidb/genji/internal/errors"
)

// Default Base64 encoder string doesn't preserve lexicographic order. This alternative
// encoder does.
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
func DecodeBool(buf []byte) (bool, error) {
	if len(buf) == 0 {
		return false, errors.New("cannot decode buffer to bool")
	}
	return buf[0] == 1, nil
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
	if len(buf) < 8 {
		return 0, errors.New("cannot decode buffer to float64")
	}
	x := binary.BigEndian.Uint64(buf)

	if (x & (1 << 63)) != 0 {
		x ^= 1 << 63
	} else {
		x ^= 1<<64 - 1
	}
	return math.Float64frombits(x), nil
}

// AppendBase64 encodes data into a custom base64 encoding. The resulting slice respects
// natural sort-ordering.
func AppendBase64(buf []byte, data []byte) ([]byte, error) {
	b := bytes.NewBuffer(buf)
	enc := base64.NewEncoder(base64Encoding, b)
	_, _ = enc.Write(data)
	_ = enc.Close()

	return b.Bytes(), nil
}

// DecodeBase64 decodes a custom base64 encoded byte slice,
// encoded with AppendBase64.
func DecodeBase64(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	dec := base64.NewDecoder(base64Encoding, bytes.NewReader(data))
	_, err := buf.ReadFrom(dec)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
