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
		enc      func([]byte) []byte
		dec      func([]byte) (interface{}, error)
	}{
		{"bool", true, func(buf []byte) []byte { return AppendBool(buf, true) }, func(buf []byte) (interface{}, error) { return DecodeBool(buf), nil }},
		{"uint64", uint64(10), func(buf []byte) []byte { return AppendUint64(buf, 10) }, func(buf []byte) (interface{}, error) { return DecodeUint64(buf) }},
		{"int64", int64(-10), func(buf []byte) []byte { return AppendInt64(buf, -10) }, func(buf []byte) (interface{}, error) { return DecodeInt64(buf) }},
		{"float64", float64(-3.14), func(buf []byte) []byte { return AppendFloat64(buf, -3.14) }, func(buf []byte) (interface{}, error) { return DecodeFloat64(buf) }},
	}

	for _, test := range tests {
		var buf []byte
		t.Run(test.name, func(t *testing.T) {
			buf = test.enc(buf[:0])
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
		enc      func([]byte, int) []byte
	}{
		{"uint64", 0, 1000, func(buf []byte, i int) []byte { return AppendUint64(buf, uint64(i)) }},
		{"int64", -1000, 1000, func(buf []byte, i int) []byte { return AppendInt64(buf, int64(i)) }},
		{"float64", -1000, 1000, func(buf []byte, i int) []byte { return AppendFloat64(buf, float64(i)) }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var prev, cur []byte
			for i := test.min; i < test.max; i++ {
				cur = test.enc(cur[:0], i)
				if prev == nil {
					prev = append(prev[:0], cur...)
					continue
				}

				require.Equal(t, -1, bytes.Compare(prev, cur))
				prev = append(prev[:0], cur...)
			}
		})
	}
}
