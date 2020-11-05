package binarysort

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOrdering(t *testing.T) {
	tests := []struct {
		name     string
		min, max int
		enc      func([]byte, int) []byte
	}{
		{"uint64", 0, 1000, func(buf []byte, i int) []byte { return AppendUint64(buf, uint64(i)) }},
		{"int64", -1000, 1000, func(buf []byte, i int) []byte { return AppendInt64(buf, int64(i)) }},
		{"float64", -1000, 1000, func(buf []byte, i int) []byte { return AppendFloat64(buf, float64(i)) }},
		{"text", -1000, 1000, func(buf []byte, i int) []byte {
			b, err := AppendBase64(nil, AppendInt64(buf, int64(i)))
			require.NoError(t, err)
			return b
		}},
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
