package document_test

import (
	"bytes"
	"testing"

	"github.com/asdine/genji/document"
	"github.com/stretchr/testify/require"
)

func TestValueString(t *testing.T) {
	tests := []struct {
		name     string
		value    document.Value
		expected string
	}{
		{"bytes", document.NewBytesValue([]byte("bar")), "[98 97 114]"},
		{"string", document.NewStringValue("bar"), "bar"},
		{"bool", document.NewBoolValue(true), "true"},
		{"uint", document.NewUintValue(10), "10"},
		{"uint8", document.NewUint8Value(10), "10"},
		{"uint16", document.NewUint16Value(10), "10"},
		{"uint32", document.NewUint32Value(10), "10"},
		{"uint64", document.NewUint64Value(10), "10"},
		{"int", document.NewIntValue(10), "10"},
		{"int8", document.NewInt8Value(10), "10"},
		{"int16", document.NewInt16Value(10), "10"},
		{"int32", document.NewInt32Value(10), "10"},
		{"int64", document.NewInt64Value(10), "10"},
		{"float64", document.NewFloat64Value(10.1), "10.1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, test.value.String())
		})
	}
}

func TestValueEncodeDecode(t *testing.T) {
	tests := []struct {
		name     string
		expected interface{}
		enc      func() []byte
		dec      func([]byte) (interface{}, error)
	}{
		{"bytes", []byte("foo"), func() []byte { return document.EncodeBytes([]byte("foo")) }, func(buf []byte) (interface{}, error) { return document.DecodeBytes(buf) }},
		{"string", "bar", func() []byte { return document.EncodeString("bar") }, func(buf []byte) (interface{}, error) { return document.DecodeString(buf) }},
		{"bool", true, func() []byte { return document.EncodeBool(true) }, func(buf []byte) (interface{}, error) { return document.DecodeBool(buf) }},
		{"uint", uint(10), func() []byte { return document.EncodeUint(10) }, func(buf []byte) (interface{}, error) { return document.DecodeUint(buf) }},
		{"uint8", uint8(10), func() []byte { return document.EncodeUint8(10) }, func(buf []byte) (interface{}, error) { return document.DecodeUint8(buf) }},
		{"uint16", uint16(10), func() []byte { return document.EncodeUint16(10) }, func(buf []byte) (interface{}, error) { return document.DecodeUint16(buf) }},
		{"uint32", uint32(10), func() []byte { return document.EncodeUint32(10) }, func(buf []byte) (interface{}, error) { return document.DecodeUint32(buf) }},
		{"uint64", uint64(10), func() []byte { return document.EncodeUint64(10) }, func(buf []byte) (interface{}, error) { return document.DecodeUint64(buf) }},
		{"int", int(-10), func() []byte { return document.EncodeInt(-10) }, func(buf []byte) (interface{}, error) { return document.DecodeInt(buf) }},
		{"int8", int8(-10), func() []byte { return document.EncodeInt8(-10) }, func(buf []byte) (interface{}, error) { return document.DecodeInt8(buf) }},
		{"int16", int16(-10), func() []byte { return document.EncodeInt16(-10) }, func(buf []byte) (interface{}, error) { return document.DecodeInt16(buf) }},
		{"int32", int32(-10), func() []byte { return document.EncodeInt32(-10) }, func(buf []byte) (interface{}, error) { return document.DecodeInt32(buf) }},
		{"int64", int64(-10), func() []byte { return document.EncodeInt64(-10) }, func(buf []byte) (interface{}, error) { return document.DecodeInt64(buf) }},
		{"float64", float64(-3.14), func() []byte { return document.EncodeFloat64(-3.14) }, func(buf []byte) (interface{}, error) { return document.DecodeFloat64(buf) }},
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
		{"uint", 0, 1000, func(i int) []byte { return document.EncodeUint(uint(i)) }},
		{"uint8", 0, 255, func(i int) []byte { return document.EncodeUint8(uint8(i)) }},
		{"uint16", 0, 1000, func(i int) []byte { return document.EncodeUint16(uint16(i)) }},
		{"uint32", 0, 1000, func(i int) []byte { return document.EncodeUint32(uint32(i)) }},
		{"uint64", 0, 1000, func(i int) []byte { return document.EncodeUint64(uint64(i)) }},
		{"int", -1000, 1000, func(i int) []byte { return document.EncodeInt(i) }},
		{"int8", -100, 100, func(i int) []byte { return document.EncodeInt8(int8(i)) }},
		{"int16", -1000, 1000, func(i int) []byte { return document.EncodeInt16(int16(i)) }},
		{"int32", -1000, 1000, func(i int) []byte { return document.EncodeInt32(int32(i)) }},
		{"int64", -1000, 1000, func(i int) []byte { return document.EncodeInt64(int64(i)) }},
		{"float64", -1000, 1000, func(i int) []byte { return document.EncodeFloat64(float64(i)) }},
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
	v := document.NewFloat64Value(3.14)
	price, err := v.Decode()
	require.NoError(t, err)
	require.Equal(t, 3.14, price)
}

func TestNew(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
	}{
		{"bytes", []byte("bar")},
		{"string", "bar"},
		{"bool", true},
		{"uint", uint(10)},
		{"uint8", uint8(10)},
		{"uint16", uint16(10)},
		{"uint32", uint32(10)},
		{"uint64", uint64(10)},
		{"int", int(10)},
		{"int8", int8(10)},
		{"int16", int16(10)},
		{"int32", int32(10)},
		{"int64", int64(10)},
		{"float64", 10.1},
		{"nil", nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v, err := document.NewValue(test.value)
			require.NoError(t, err)
			i, err := v.Decode()
			require.NoError(t, err)
			require.Equal(t, test.value, i)
		})
	}
}

func TestDecodeToBytes(t *testing.T) {
	tests := []struct {
		name     string
		v        document.Value
		fails    bool
		expected []byte
	}{
		{"bytes", document.NewBytesValue([]byte("bar")), false, []byte("bar")},
		{"string", document.NewStringValue("bar"), false, []byte("bar")},
		{"bool", document.NewBoolValue(true), false, document.NewBoolValue(true).Data},
		{"uint", document.NewUintValue(10), false, document.NewUintValue(10).Data},
		{"uint8", document.NewUint8Value(10), false, document.NewUint8Value(10).Data},
		{"uint16", document.NewUint16Value(10), false, document.NewUint16Value(10).Data},
		{"uint32", document.NewUint32Value(10), false, document.NewUint32Value(10).Data},
		{"uint64", document.NewUint64Value(10), false, document.NewUint64Value(10).Data},
		{"int", document.NewIntValue(10), false, document.NewIntValue(10).Data},
		{"int8", document.NewInt8Value(10), false, document.NewInt8Value(10).Data},
		{"int16", document.NewInt16Value(10), false, document.NewInt16Value(10).Data},
		{"int32", document.NewInt32Value(10), false, document.NewInt32Value(10).Data},
		{"int64", document.NewInt64Value(10), false, document.NewInt64Value(10).Data},
		{"float64", document.NewFloat64Value(10.1), false, document.NewFloat64Value(10.1).Data},
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
		v        document.Value
		fails    bool
		expected string
	}{
		{"bytes", document.NewBytesValue([]byte("bar")), false, "bar"},
		{"string", document.NewStringValue("bar"), false, "bar"},
		{"bool", document.NewBoolValue(true), true, ""},
		{"uint", document.NewUintValue(10), true, ""},
		{"uint8", document.NewUint8Value(10), true, ""},
		{"uint16", document.NewUint16Value(10), true, ""},
		{"uint32", document.NewUint32Value(10), true, ""},
		{"uint64", document.NewUint64Value(10), true, ""},
		{"int", document.NewIntValue(10), true, ""},
		{"int8", document.NewInt8Value(10), true, ""},
		{"int16", document.NewInt16Value(10), true, ""},
		{"int32", document.NewInt32Value(10), true, ""},
		{"int64", document.NewInt64Value(10), true, ""},
		{"float64", document.NewFloat64Value(10.1), true, ""},
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
		v        document.Value
		fails    bool
		expected bool
	}{
		{"bytes", document.NewBytesValue([]byte("bar")), false, true},
		{"zero bytes", document.NewBytesValue([]byte("")), false, false},
		{"string", document.NewStringValue("bar"), false, true},
		{"zero string", document.NewStringValue(""), false, false},
		{"bool", document.NewBoolValue(true), false, true},
		{"zero bool", document.NewBoolValue(false), false, false},
		{"uint", document.NewUintValue(10), false, true},
		{"zero uint", document.NewUintValue(0), false, false},
		{"uint8", document.NewUint8Value(10), false, true},
		{"zero uint8", document.NewUint8Value(0), false, false},
		{"uint16", document.NewUint16Value(10), false, true},
		{"zero uint16", document.NewUint16Value(0), false, false},
		{"uint32", document.NewUint32Value(10), false, true},
		{"zero uint32", document.NewUint32Value(0), false, false},
		{"uint64", document.NewUint64Value(10), false, true},
		{"zero uint64", document.NewUint64Value(0), false, false},
		{"int", document.NewIntValue(10), false, true},
		{"zero int", document.NewIntValue(0), false, false},
		{"int8", document.NewInt8Value(10), false, true},
		{"zero int8", document.NewInt8Value(0), false, false},
		{"int16", document.NewInt16Value(10), false, true},
		{"zero int16", document.NewInt16Value(0), false, false},
		{"int32", document.NewInt32Value(10), false, true},
		{"zero int32", document.NewInt32Value(0), false, false},
		{"int64", document.NewInt64Value(10), false, true},
		{"zero int64", document.NewInt64Value(0), false, false},
		{"float64", document.NewFloat64Value(10.1), false, true},
		{"zero float64", document.NewFloat64Value(0), false, false},
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
		v        document.Value
		fails    bool
		expected int64
	}{
		{"bytes", document.NewBytesValue([]byte("bar")), true, 0},
		{"string", document.NewStringValue("bar"), true, 0},
		{"bool", document.NewBoolValue(true), true, 0},
		{"uint", document.NewUintValue(10), false, 10},
		{"uint8", document.NewUint8Value(10), false, 10},
		{"uint16", document.NewUint16Value(10), false, 10},
		{"uint32", document.NewUint32Value(10), false, 10},
		{"uint64", document.NewUint64Value(10), false, 10},
		{"int", document.NewIntValue(10), false, 10},
		{"int8", document.NewInt8Value(10), false, 10},
		{"int16", document.NewInt16Value(10), false, 10},
		{"int32", document.NewInt32Value(10), false, 10},
		{"int64", document.NewInt64Value(10), false, 10},
		{"float64", document.NewFloat64Value(10), false, 10},
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
		t.Run(test.name+" to float64", func(t *testing.T) {
			res, err := test.v.DecodeToFloat64()
			check(t, res, err, test.fails, float64(test.expected))
		})
	}

	t.Run("float64/precision loss", func(t *testing.T) {
		_, err := document.NewFloat64Value(10.4).DecodeToUint16()
		require.Error(t, err)
		_, err = document.NewFloat64Value(10.4).ConvertTo(document.Int32Value)
		require.Error(t, err)
	})
}

func TestTypeFromGoType(t *testing.T) {
	tests := []struct {
		goType   string
		expected document.ValueType
	}{
		{"[]byte", document.BytesValue},
		{"struct", document.DocumentValue},
	}

	for _, test := range tests {
		t.Run(test.goType, func(t *testing.T) {
			require.Equal(t, test.expected, document.NewValueTypeFromGoType(test.goType))
		})
	}
}
