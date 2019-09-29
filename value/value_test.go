package value_test

import (
	"bytes"
	"testing"

	"github.com/asdine/genji/value"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecode(t *testing.T) {
	tests := []struct {
		name     string
		expected interface{}
		enc      func() []byte
		dec      func([]byte) (interface{}, error)
	}{
		{"bytes", []byte("foo"), func() []byte { return value.EncodeBytes([]byte("foo")) }, func(buf []byte) (interface{}, error) { return value.DecodeBytes(buf) }},
		{"string", "bar", func() []byte { return value.EncodeString("bar") }, func(buf []byte) (interface{}, error) { return value.DecodeString(buf) }},
		{"bool", true, func() []byte { return value.EncodeBool(true) }, func(buf []byte) (interface{}, error) { return value.DecodeBool(buf) }},
		{"uint", uint(10), func() []byte { return value.EncodeUint(10) }, func(buf []byte) (interface{}, error) { return value.DecodeUint(buf) }},
		{"uint8", uint8(10), func() []byte { return value.EncodeUint8(10) }, func(buf []byte) (interface{}, error) { return value.DecodeUint8(buf) }},
		{"uint16", uint16(10), func() []byte { return value.EncodeUint16(10) }, func(buf []byte) (interface{}, error) { return value.DecodeUint16(buf) }},
		{"uint32", uint32(10), func() []byte { return value.EncodeUint32(10) }, func(buf []byte) (interface{}, error) { return value.DecodeUint32(buf) }},
		{"uint64", uint64(10), func() []byte { return value.EncodeUint64(10) }, func(buf []byte) (interface{}, error) { return value.DecodeUint64(buf) }},
		{"int", int(-10), func() []byte { return value.EncodeInt(-10) }, func(buf []byte) (interface{}, error) { return value.DecodeInt(buf) }},
		{"int8", int8(-10), func() []byte { return value.EncodeInt8(-10) }, func(buf []byte) (interface{}, error) { return value.DecodeInt8(buf) }},
		{"int16", int16(-10), func() []byte { return value.EncodeInt16(-10) }, func(buf []byte) (interface{}, error) { return value.DecodeInt16(buf) }},
		{"int32", int32(-10), func() []byte { return value.EncodeInt32(-10) }, func(buf []byte) (interface{}, error) { return value.DecodeInt32(buf) }},
		{"int64", int64(-10), func() []byte { return value.EncodeInt64(-10) }, func(buf []byte) (interface{}, error) { return value.DecodeInt64(buf) }},
		{"float32", float32(-3.14), func() []byte { return value.EncodeFloat32(-3.14) }, func(buf []byte) (interface{}, error) { return value.DecodeFloat32(buf) }},
		{"float64", float64(-3.14), func() []byte { return value.EncodeFloat64(-3.14) }, func(buf []byte) (interface{}, error) { return value.DecodeFloat64(buf) }},
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
		{"uint", 0, 1000, func(i int) []byte { return value.EncodeUint(uint(i)) }},
		{"uint8", 0, 255, func(i int) []byte { return value.EncodeUint8(uint8(i)) }},
		{"uint16", 0, 1000, func(i int) []byte { return value.EncodeUint16(uint16(i)) }},
		{"uint32", 0, 1000, func(i int) []byte { return value.EncodeUint32(uint32(i)) }},
		{"uint64", 0, 1000, func(i int) []byte { return value.EncodeUint64(uint64(i)) }},
		{"int", -1000, 1000, func(i int) []byte { return value.EncodeInt(i) }},
		{"int8", -100, 100, func(i int) []byte { return value.EncodeInt8(int8(i)) }},
		{"int16", -1000, 1000, func(i int) []byte { return value.EncodeInt16(int16(i)) }},
		{"int32", -1000, 1000, func(i int) []byte { return value.EncodeInt32(int32(i)) }},
		{"int64", -1000, 1000, func(i int) []byte { return value.EncodeInt64(int64(i)) }},
		{"float32", -1000, 1000, func(i int) []byte { return value.EncodeFloat32(float32(i)) }},
		{"float64", -1000, 1000, func(i int) []byte { return value.EncodeFloat64(float64(i)) }},
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

func TestDecode(t *testing.T) {
	v := value.NewFloat64(3.14)
	price, err := v.Decode()
	require.NoError(t, err)
	require.Equal(t, 3.14, price)
}

func TestFieldString(t *testing.T) {
	tests := []struct {
		name     string
		value    value.Value
		expected string
	}{
		{"bytes", value.NewBytes([]byte("bar")), "[98 97 114]"},
		{"string", value.NewString("bar"), "bar"},
		{"uint", value.NewUint(10), "10"},
		{"uint8", value.NewUint8(10), "10"},
		{"uint16", value.NewUint16(10), "10"},
		{"uint32", value.NewUint32(10), "10"},
		{"uint64", value.NewUint64(10), "10"},
		{"int", value.NewInt(10), "10"},
		{"int8", value.NewInt8(10), "10"},
		{"int16", value.NewInt16(10), "10"},
		{"int32", value.NewInt32(10), "10"},
		{"int64", value.NewInt64(10), "10"},
		{"float32", value.NewFloat32(10.1), "10.1"},
		{"float64", value.NewFloat64(10.1), "10.1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, test.value.String())
		})
	}

}
