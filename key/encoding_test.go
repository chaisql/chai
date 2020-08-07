package key

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValueEncodeDecode(t *testing.T) {
	tests := []struct {
		name     string
		expected interface{}
		enc      func() []byte
		dec      func([]byte) (interface{}, error)
	}{
		{"string", "bar", func() []byte { return EncodeString("bar") }, func(buf []byte) (interface{}, error) { return DecodeString(buf) }},
		{"bool", true, func() []byte { return EncodeBool(true) }, func(buf []byte) (interface{}, error) { return DecodeBool(buf) }},
		{"uint64", uint64(10), func() []byte { return EncodeUint64(10) }, func(buf []byte) (interface{}, error) { return DecodeUint64(buf) }},
		{"int64", int64(-10), func() []byte { return EncodeInt64(-10) }, func(buf []byte) (interface{}, error) { return DecodeInt64(buf) }},
		{"float64", float64(-3.14), func() []byte { return EncodeFloat64(-3.14) }, func(buf []byte) (interface{}, error) { return DecodeFloat64(buf) }},
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
		{"uint64", 0, 1000, func(i int) []byte { return EncodeUint64(uint64(i)) }},
		{"int64", -1000, 1000, func(i int) []byte { return EncodeInt64(int64(i)) }},
		{"float64", -1000, 1000, func(i int) []byte { return EncodeFloat64(float64(i)) }},
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
