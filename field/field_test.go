package field_test

import (
	"bytes"
	"testing"

	"github.com/asdine/genji/field"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecode(t *testing.T) {
	tests := []struct {
		name     string
		expected interface{}
		enc      func() []byte
		dec      func([]byte) (interface{}, error)
	}{
		{"bytes", []byte("foo"), func() []byte { return field.EncodeBytes([]byte("foo")) }, func(buf []byte) (interface{}, error) { return field.DecodeBytes(buf) }},
		{"string", "bar", func() []byte { return field.EncodeString("bar") }, func(buf []byte) (interface{}, error) { return field.DecodeString(buf) }},
		{"bool", true, func() []byte { return field.EncodeBool(true) }, func(buf []byte) (interface{}, error) { return field.DecodeBool(buf) }},
		{"uint", uint(10), func() []byte { return field.EncodeUint(10) }, func(buf []byte) (interface{}, error) { return field.DecodeUint(buf) }},
		{"uint8", uint8(10), func() []byte { return field.EncodeUint8(10) }, func(buf []byte) (interface{}, error) { return field.DecodeUint8(buf) }},
		{"uint16", uint16(10), func() []byte { return field.EncodeUint16(10) }, func(buf []byte) (interface{}, error) { return field.DecodeUint16(buf) }},
		{"uint32", uint32(10), func() []byte { return field.EncodeUint32(10) }, func(buf []byte) (interface{}, error) { return field.DecodeUint32(buf) }},
		{"uint64", uint64(10), func() []byte { return field.EncodeUint64(10) }, func(buf []byte) (interface{}, error) { return field.DecodeUint64(buf) }},
		{"int", int(-10), func() []byte { return field.EncodeInt(-10) }, func(buf []byte) (interface{}, error) { return field.DecodeInt(buf) }},
		{"int8", int8(-10), func() []byte { return field.EncodeInt8(-10) }, func(buf []byte) (interface{}, error) { return field.DecodeInt8(buf) }},
		{"int16", int16(-10), func() []byte { return field.EncodeInt16(-10) }, func(buf []byte) (interface{}, error) { return field.DecodeInt16(buf) }},
		{"int32", int32(-10), func() []byte { return field.EncodeInt32(-10) }, func(buf []byte) (interface{}, error) { return field.DecodeInt32(buf) }},
		{"int64", int64(-10), func() []byte { return field.EncodeInt64(-10) }, func(buf []byte) (interface{}, error) { return field.DecodeInt64(buf) }},
		{"float32", float32(-3.14), func() []byte { return field.EncodeFloat32(-3.14) }, func(buf []byte) (interface{}, error) { return field.DecodeFloat32(buf) }},
		{"float64", float64(-3.14), func() []byte { return field.EncodeFloat64(-3.14) }, func(buf []byte) (interface{}, error) { return field.DecodeFloat64(buf) }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buf := test.enc()
			actual, err := test.dec(buf)
			require.NoError(t, err)
			require.Equal(t, test.expected, actual)
		})
	}
}

const Rng = 1000

func TestOrdering(t *testing.T) {
	tests := []struct {
		name     string
		min, max int
		enc      func(int) []byte
	}{
		{"uint", 0, 1000, func(i int) []byte { return field.EncodeUint(uint(i)) }},
		{"uint8", 0, 255, func(i int) []byte { return field.EncodeUint8(uint8(i)) }},
		{"uint16", 0, 1000, func(i int) []byte { return field.EncodeUint16(uint16(i)) }},
		{"uint32", 0, 1000, func(i int) []byte { return field.EncodeUint32(uint32(i)) }},
		{"uint64", 0, 1000, func(i int) []byte { return field.EncodeUint64(uint64(i)) }},
		{"int", -1000, 1000, func(i int) []byte { return field.EncodeInt(i) }},
		{"int8", -100, 100, func(i int) []byte { return field.EncodeInt8(int8(i)) }},
		{"int16", -1000, 1000, func(i int) []byte { return field.EncodeInt16(int16(i)) }},
		{"int32", -1000, 1000, func(i int) []byte { return field.EncodeInt32(int32(i)) }},
		{"int64", -1000, 1000, func(i int) []byte { return field.EncodeInt64(int64(i)) }},
		{"float32", -1000, 1000, func(i int) []byte { return field.EncodeFloat32(float32(i)) }},
		{"float64", -1000, 1000, func(i int) []byte { return field.EncodeFloat64(float64(i)) }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var prev []byte
			for i := test.min; i < test.max; i++ {
				cur := test.enc(i)
				if prev == nil {
					prev = cur
					continue
				}

				require.Equal(t, -1, bytes.Compare(prev, cur))
				prev = cur
			}
		})
	}
}
