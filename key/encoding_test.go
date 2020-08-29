package key

import (
	"bytes"
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
}
