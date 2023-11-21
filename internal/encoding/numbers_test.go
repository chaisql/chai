package encoding_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/genjidb/genji/internal/encoding"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeInt(t *testing.T) {
	tests := []struct {
		input int64
		want  []byte
	}{
		// small numbers from -32 to 32 fit in a single byte
		{-32, []byte{byte(encoding.IntSmallValue)}},
		{-10, []byte{byte(encoding.IntSmallValue) + 22}},
		{0, []byte{byte(encoding.IntSmallValue) + 32}},
		{10, []byte{byte(encoding.IntSmallValue) + 42}},
		{31, []byte{byte(encoding.IntSmallValue) + 63}},

		// int8
		{math.MinInt8, []byte{byte(encoding.Int8Value), 0x00}},
		{-33, []byte{byte(encoding.Int8Value), 0x5f}},
		{-40, []byte{byte(encoding.Int8Value), 0x58}},

		// int16
		{math.MinInt16, []byte{byte(encoding.Int16Value), 0x00, 0x00}},
		{-400, []byte{byte(encoding.Int16Value), 0x7e, 0x70}},

		// int32
		{math.MinInt32, []byte{byte(encoding.Int32Value), 0x00, 0x00, 0x00, 0x00}},
		{-4000000, []byte{byte(encoding.Int32Value), 0x7f, 0xc2, 0xf7, 0x0}},

		// int64
		{math.MinInt64, []byte{byte(encoding.Int64Value), 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{-4000000000000, []byte{byte(encoding.Int64Value), 0x7f, 0xff, 0xfc, 0x5c, 0xad, 0x6b, 0xc0, 0x0}},

		// uint8
		{128, []byte{byte(encoding.Uint8Value), 0x80}},
		{math.MaxUint8, []byte{byte(encoding.Uint8Value), 0xff}},

		// uint16
		{math.MaxUint16, []byte{byte(encoding.Uint16Value), 0xff, 0xff}},

		// uint32
		{math.MaxUint32, []byte{byte(encoding.Uint32Value), 0xff, 0xff, 0xff, 0xff}},

		// uint64
		{math.MaxUint32 + 1, []byte{byte(encoding.Uint64Value), 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0}},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%d", test.input), func(t *testing.T) {
			got := encoding.EncodeInt(nil, test.input)
			require.Equal(t, test.want, got)

			x, _ := encoding.DecodeInt(got)
			require.Equal(t, test.input, x)
		})
	}
}

func TestEncodeDecodeFloat(t *testing.T) {
	tests := []struct {
		input float64
		want  []byte
	}{
		{-3.14, []byte{0x5a, 0x3f, 0xf6, 0xe1, 0x47, 0xae, 0x14, 0x7a, 0xe0}},
		{-3, []byte{0x2d}},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%f", test.input), func(t *testing.T) {
			got := encoding.EncodeFloat(nil, test.input)
			require.Equal(t, test.want, got)

			x, _ := encoding.DecodeFloat(got)
			require.Equal(t, test.input, x)
		})
	}
}
