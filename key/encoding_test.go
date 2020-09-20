package key

import (
	"bytes"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/genjidb/genji/document"
	"github.com/stretchr/testify/require"
)

func TestValueEncodeDecode(t *testing.T) {
	tests := []struct {
		name string
		v    document.Value
	}{
		{"null", document.NewNullValue()},
		{"bool", document.NewBoolValue(true)},
		{"integer", document.NewIntegerValue(-10)},
		{"double", document.NewDoubleValue(-3.14)},
		{"text", document.NewTextValue("foo")},
		{"blob", document.NewBlobValue([]byte("bar"))},
		{"duration", document.NewDurationValue(10 * time.Second)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b := AppendValue(nil, test.v)
			got, err := DecodeValue(test.v.Type, b)
			require.NoError(t, err)
			require.Equal(t, test.v, got)
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

	t.Run("Ordered ints and floats", func(t *testing.T) {
		ints := []int64{
			40, 7000, math.MaxInt64 - 1, math.MaxInt64,
		}
		floats := []float64{
			-1000.4, 40, 7000.3, math.MaxFloat64 - 1, math.MaxFloat64,
		}

		var encoded [][]byte
		for _, nb := range ints {
			encoded = append(encoded, AppendInt64(nil, nb))
		}
		for _, nb := range floats {
			encoded = append(encoded, AppendIntSortedFloat(nil, nb))
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
			if len(enc) == 16 {
				x, err = DecodeFloat64(enc[8:])
			} else {
				x, err = DecodeInt64(enc)
			}
			require.NoError(t, err)
			require.Equal(t, want[i], x)
		}
	})
}
