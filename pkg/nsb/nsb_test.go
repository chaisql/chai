package nsb

import (
	"bytes"
	"math"
	"sort"
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

	t.Run("Ordered ints and floats", func(t *testing.T) {
		ints := []int64{
			40, 7000, math.MaxInt64 - 1, math.MaxInt64,
		}
		floats := []float64{
			-1000.4, 40, 7000.3, math.MaxFloat64 - 1, math.MaxFloat64,
		}

		var encoded [][]byte
		for _, nb := range ints {
			enc, err := AppendIntNumber(nil, nb)
			require.NoError(t, err)
			encoded = append(encoded, enc)
		}
		for _, nb := range floats {
			enc, err := AppendFloatNumber(nil, nb)
			require.NoError(t, err)
			encoded = append(encoded, enc)
		}

		sort.Slice(encoded, func(i, j int) bool {
			return bytes.Compare(encoded[i], encoded[j]) < 0
		})

		want := []interface{}{
			-1000.4,
			int64(40),
			float64(40),
			int64(7000),
			7000.3,
			int64(math.MaxInt64 - 1),
			int64(math.MaxInt64),
			math.MaxFloat64 - 1,
			math.MaxFloat64,
		}

		var err error
		var x interface{}

		for i, enc := range encoded {
			if bytes.Equal(enc[8:], []byte{0, 0, 0, 0, 0, 0, 0, 0}) {
				x, err = DecodeInt64(enc[:8])
			} else {
				x, err = DecodeFloat64(enc[8:])
			}
			require.NoError(t, err)
			require.Equal(t, want[i], x)
		}
	})
}
