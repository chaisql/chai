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
		{"bool", value.NewBool(true), "true"},
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

func TestDecodeToBytes(t *testing.T) {
	tests := []struct {
		name     string
		v        value.Value
		fails    bool
		expected []byte
	}{
		{"bytes", value.NewBytes([]byte("bar")), false, []byte("bar")},
		{"string", value.NewString("bar"), false, []byte("bar")},
		{"bool", value.NewBool(true), false, value.NewBool(true).Data},
		{"uint", value.NewUint(10), false, value.NewUint(10).Data},
		{"uint8", value.NewUint8(10), false, value.NewUint8(10).Data},
		{"uint16", value.NewUint16(10), false, value.NewUint16(10).Data},
		{"uint32", value.NewUint32(10), false, value.NewUint32(10).Data},
		{"uint64", value.NewUint64(10), false, value.NewUint64(10).Data},
		{"int", value.NewInt(10), false, value.NewInt(10).Data},
		{"int8", value.NewInt8(10), false, value.NewInt8(10).Data},
		{"int16", value.NewInt16(10), false, value.NewInt16(10).Data},
		{"int32", value.NewInt32(10), false, value.NewInt32(10).Data},
		{"int64", value.NewInt64(10), false, value.NewInt64(10).Data},
		{"float32", value.NewFloat32(10.1), false, value.NewFloat32(10.1).Data},
		{"float64", value.NewFloat64(10.1), false, value.NewFloat64(10.1).Data},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.DecodeToBytes()
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestDecodeToString(t *testing.T) {
	tests := []struct {
		name     string
		v        value.Value
		fails    bool
		expected string
	}{
		{"bytes", value.NewBytes([]byte("bar")), false, "bar"},
		{"string", value.NewString("bar"), false, "bar"},
		{"bool", value.NewBool(true), true, ""},
		{"uint", value.NewUint(10), true, ""},
		{"uint8", value.NewUint8(10), true, ""},
		{"uint16", value.NewUint16(10), true, ""},
		{"uint32", value.NewUint32(10), true, ""},
		{"uint64", value.NewUint64(10), true, ""},
		{"int", value.NewInt(10), true, ""},
		{"int8", value.NewInt8(10), true, ""},
		{"int16", value.NewInt16(10), true, ""},
		{"int32", value.NewInt32(10), true, ""},
		{"int64", value.NewInt64(10), true, ""},
		{"float32", value.NewFloat32(10.1), true, ""},
		{"float64", value.NewFloat64(10.1), true, ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.DecodeToString()
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestDecodeToBool(t *testing.T) {
	tests := []struct {
		name     string
		v        value.Value
		fails    bool
		expected bool
	}{
		{"bytes", value.NewBytes([]byte("bar")), false, true},
		{"zero bytes", value.NewBytes([]byte("")), false, false},
		{"string", value.NewString("bar"), false, true},
		{"zero string", value.NewString(""), false, false},
		{"bool", value.NewBool(true), false, true},
		{"zero bool", value.NewBool(false), false, false},
		{"uint", value.NewUint(10), false, true},
		{"zero uint", value.NewUint(0), false, false},
		{"uint8", value.NewUint8(10), false, true},
		{"zero uint8", value.NewUint8(0), false, false},
		{"uint16", value.NewUint16(10), false, true},
		{"zero uint16", value.NewUint16(0), false, false},
		{"uint32", value.NewUint32(10), false, true},
		{"zero uint32", value.NewUint32(0), false, false},
		{"uint64", value.NewUint64(10), false, true},
		{"zero uint64", value.NewUint64(0), false, false},
		{"int", value.NewInt(10), false, true},
		{"zero int", value.NewInt(0), false, false},
		{"int8", value.NewInt8(10), false, true},
		{"zero int8", value.NewInt8(0), false, false},
		{"int16", value.NewInt16(10), false, true},
		{"zero int16", value.NewInt16(0), false, false},
		{"int32", value.NewInt32(10), false, true},
		{"zero int32", value.NewInt32(0), false, false},
		{"int64", value.NewInt64(10), false, true},
		{"zero int64", value.NewInt64(0), false, false},
		{"float32", value.NewFloat32(10.1), false, true},
		{"zero float32", value.NewFloat32(0), false, false},
		{"float64", value.NewFloat64(10.1), false, true},
		{"zero float64", value.NewFloat64(0), false, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.DecodeToBool()
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestDecodeToNumber(t *testing.T) {
	tests := []struct {
		name     string
		v        value.Value
		fails    bool
		expected int64
	}{
		{"bytes", value.NewBytes([]byte("bar")), true, 0},
		{"string", value.NewString("bar"), true, 0},
		{"bool", value.NewBool(true), true, 0},
		{"uint", value.NewUint(10), false, 10},
		{"uint8", value.NewUint8(10), false, 10},
		{"uint16", value.NewUint16(10), false, 10},
		{"uint32", value.NewUint32(10), false, 10},
		{"uint64", value.NewUint64(10), false, 10},
		{"int", value.NewInt(10), false, 10},
		{"int8", value.NewInt8(10), false, 10},
		{"int16", value.NewInt16(10), false, 10},
		{"int32", value.NewInt32(10), false, 10},
		{"int64", value.NewInt64(10), false, 10},
		{"float32", value.NewFloat32(10), false, 10},
		{"float64", value.NewFloat64(10), false, 10},
	}

	check := func(t *testing.T, res interface{}, err error, fails bool, expected interface{}) {
		if fails {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, expected, res)
		}
	}

	for _, test := range tests {
		t.Run(test.name+" to uint", func(t *testing.T) {
			res, err := test.v.DecodeToUint()
			check(t, res, err, test.fails, uint(test.expected))
		})
		t.Run(test.name+" to uint8", func(t *testing.T) {
			res, err := test.v.DecodeToUint8()
			check(t, res, err, test.fails, uint8(test.expected))
		})
		t.Run(test.name+" to uint16", func(t *testing.T) {
			res, err := test.v.DecodeToUint16()
			check(t, res, err, test.fails, uint16(test.expected))
		})
		t.Run(test.name+" to uint32", func(t *testing.T) {
			res, err := test.v.DecodeToUint32()
			check(t, res, err, test.fails, uint32(test.expected))
		})
		t.Run(test.name+" to uint64", func(t *testing.T) {
			res, err := test.v.DecodeToUint64()
			check(t, res, err, test.fails, uint64(test.expected))
		})
		t.Run(test.name+" to int", func(t *testing.T) {
			res, err := test.v.DecodeToInt()
			check(t, res, err, test.fails, int(test.expected))
		})
		t.Run(test.name+" to int8", func(t *testing.T) {
			res, err := test.v.DecodeToInt8()
			check(t, res, err, test.fails, int8(test.expected))
		})
		t.Run(test.name+" to int16", func(t *testing.T) {
			res, err := test.v.DecodeToInt16()
			check(t, res, err, test.fails, int16(test.expected))
		})
		t.Run(test.name+" to int32", func(t *testing.T) {
			res, err := test.v.DecodeToInt32()
			check(t, res, err, test.fails, int32(test.expected))
		})
		t.Run(test.name+" to int64", func(t *testing.T) {
			res, err := test.v.DecodeToInt64()
			check(t, res, err, test.fails, int64(test.expected))
		})
		t.Run(test.name+" to float32", func(t *testing.T) {
			res, err := test.v.DecodeToFloat32()
			check(t, res, err, test.fails, float32(test.expected))
		})
		t.Run(test.name+" to float64", func(t *testing.T) {
			res, err := test.v.DecodeToFloat64()
			check(t, res, err, test.fails, float64(test.expected))
		})
	}
}
