package document_test

import (
	"fmt"
	"math"
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

func TestNewValue(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
	}{
		{"bytes", []byte("bar")},
		{"string", []byte("bar")},
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
			require.Equal(t, test.value, v.V)
		})
	}
}

func TestConvertToBytes(t *testing.T) {
	tests := []struct {
		name     string
		v        document.Value
		fails    bool
		expected []byte
	}{
		{"bytes", document.NewBytesValue([]byte("bar")), false, []byte("bar")},
		{"string", document.NewStringValue("bar"), false, []byte("bar")},
		{"null", document.NewNullValue(), false, nil},
		{"bool", document.NewBoolValue(true), true, nil},
		{"uint", document.NewUintValue(10), true, nil},
		{"uint8", document.NewUint8Value(10), true, nil},
		{"uint16", document.NewUint16Value(10), true, nil},
		{"uint32", document.NewUint32Value(10), true, nil},
		{"uint64", document.NewUint64Value(10), true, nil},
		{"int", document.NewIntValue(10), true, nil},
		{"int8", document.NewInt8Value(10), true, nil},
		{"int16", document.NewInt16Value(10), true, nil},
		{"int32", document.NewInt32Value(10), true, nil},
		{"int64", document.NewInt64Value(10), true, nil},
		{"float64", document.NewFloat64Value(10.1), true, nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.ConvertToBytes()
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestConvertToString(t *testing.T) {
	tests := []struct {
		name     string
		v        document.Value
		fails    bool
		expected string
	}{
		{"bytes", document.NewBytesValue([]byte("bar")), false, "bar"},
		{"string", document.NewStringValue("bar"), false, "bar"},
		{"null", document.NewNullValue(), false, ""},
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
			res, err := test.v.ConvertToString()
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestConvertToBool(t *testing.T) {
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
		{"null", document.NewNullValue(), false, false},
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
			res, err := test.v.ConvertToBool()
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestConvertToNumber(t *testing.T) {
	tests := []struct {
		name     string
		v        document.Value
		fails    bool
		expected int64
	}{
		{"bytes", document.NewBytesValue([]byte("bar")), true, 0},
		{"string", document.NewStringValue("bar"), true, 0},
		{"bool", document.NewBoolValue(true), false, 1},
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
		{"null", document.NewNullValue(), false, 0},
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
			res, err := test.v.ConvertToUint()
			check(t, res, err, test.fails, uint(test.expected))
		})
		t.Run(test.name+" to uint8", func(t *testing.T) {
			res, err := test.v.ConvertToUint8()
			check(t, res, err, test.fails, uint8(test.expected))
		})
		t.Run(test.name+" to uint16", func(t *testing.T) {
			res, err := test.v.ConvertToUint16()
			check(t, res, err, test.fails, uint16(test.expected))
		})
		t.Run(test.name+" to uint32", func(t *testing.T) {
			res, err := test.v.ConvertToUint32()
			check(t, res, err, test.fails, uint32(test.expected))
		})
		t.Run(test.name+" to uint64", func(t *testing.T) {
			res, err := test.v.ConvertToUint64()
			check(t, res, err, test.fails, uint64(test.expected))
		})
		t.Run(test.name+" to int", func(t *testing.T) {
			res, err := test.v.ConvertToInt()
			check(t, res, err, test.fails, int(test.expected))
		})
		t.Run(test.name+" to int8", func(t *testing.T) {
			res, err := test.v.ConvertToInt8()
			check(t, res, err, test.fails, int8(test.expected))
		})
		t.Run(test.name+" to int16", func(t *testing.T) {
			res, err := test.v.ConvertToInt16()
			check(t, res, err, test.fails, int16(test.expected))
		})
		t.Run(test.name+" to int32", func(t *testing.T) {
			res, err := test.v.ConvertToInt32()
			check(t, res, err, test.fails, int32(test.expected))
		})
		t.Run(test.name+" to int64", func(t *testing.T) {
			res, err := test.v.ConvertToInt64()
			check(t, res, err, test.fails, int64(test.expected))
		})
		t.Run(test.name+" to float64", func(t *testing.T) {
			res, err := test.v.ConvertToFloat64()
			check(t, res, err, test.fails, float64(test.expected))
		})
	}

	t.Run("float64/precision loss", func(t *testing.T) {
		_, err := document.NewFloat64Value(10.4).ConvertToUint16()
		require.Error(t, err)
		_, err = document.NewFloat64Value(10.4).ConvertTo(document.Int32Value)
		require.Error(t, err)
	})

	t.Run("uints/negative numbers", func(t *testing.T) {
		_, err := document.NewInt16Value(-10).ConvertToUint()
		require.Error(t, err)
		_, err = document.NewInt16Value(-10).ConvertToUint8()
		require.Error(t, err)
		_, err = document.NewInt16Value(-10).ConvertToUint16()
		require.Error(t, err)
		_, err = document.NewInt16Value(-10).ConvertToUint32()
		require.Error(t, err)
		_, err = document.NewInt16Value(-10).ConvertToUint64()
		require.Error(t, err)
		_, err = document.NewFloat64Value(-10).ConvertTo(document.Uint32Value)
		require.Error(t, err)
	})

	t.Run("ints/overflow", func(t *testing.T) {
		tests := []struct {
			from, to document.ValueType
			x        interface{}
		}{
			{document.UintValue, document.IntValue, uint(math.MaxUint64)},
			{document.Uint8Value, document.Int8Value, uint8(math.MaxUint8)},
			{document.Uint16Value, document.Int16Value, uint16(math.MaxUint16)},
			{document.Uint32Value, document.Int32Value, uint32(math.MaxUint32)},
			{document.Uint64Value, document.Int64Value, uint64(math.MaxUint64)},
			{document.Float64Value, document.Int64Value, float64(math.MaxFloat64)},
			{document.Int16Value, document.Int8Value, int16(math.MaxInt16)},
			{document.Int32Value, document.Int16Value, int32(math.MaxInt32)},
			{document.Int64Value, document.Int32Value, int64(math.MaxInt64)},
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("%s/%s", test.from, test.to), func(t *testing.T) {
				_, err := document.Value{Type: test.from, V: test.x}.ConvertTo(test.to)
				require.Error(t, err)
			})
		}
	})
}

func TestConvertToDocument(t *testing.T) {
	tests := []struct {
		name     string
		v        document.Value
		fails    bool
		expected document.Document
	}{
		{"null", document.NewNullValue(), false, document.NewFieldBuffer()},
		{"document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewInt16Value(10))), false, document.NewFieldBuffer().Add("a", document.NewInt16Value(10))},
		{"bytes", document.NewBytesValue([]byte("bar")), true, nil},
		{"string", document.NewStringValue("bar"), true, nil},
		{"bool", document.NewBoolValue(true), true, nil},
		{"uint", document.NewUintValue(10), true, nil},
		{"uint8", document.NewUint8Value(10), true, nil},
		{"uint16", document.NewUint16Value(10), true, nil},
		{"uint32", document.NewUint32Value(10), true, nil},
		{"uint64", document.NewUint64Value(10), true, nil},
		{"int", document.NewIntValue(10), true, nil},
		{"int8", document.NewInt8Value(10), true, nil},
		{"int16", document.NewInt16Value(10), true, nil},
		{"int32", document.NewInt32Value(10), true, nil},
		{"int64", document.NewInt64Value(10), true, nil},
		{"float64", document.NewFloat64Value(10), true, nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.ConvertToDocument()
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestConvertToArray(t *testing.T) {
	tests := []struct {
		name     string
		v        document.Value
		fails    bool
		expected document.Array
	}{
		{"null", document.NewNullValue(), false, document.NewValueBuffer()},
		{"document", document.NewArrayValue(document.NewValueBuffer().Append(document.NewInt16Value(10))), false, document.NewValueBuffer().Append(document.NewInt16Value(10))},
		{"bytes", document.NewBytesValue([]byte("bar")), true, nil},
		{"string", document.NewStringValue("bar"), true, nil},
		{"bool", document.NewBoolValue(true), true, nil},
		{"uint", document.NewUintValue(10), true, nil},
		{"uint8", document.NewUint8Value(10), true, nil},
		{"uint16", document.NewUint16Value(10), true, nil},
		{"uint32", document.NewUint32Value(10), true, nil},
		{"uint64", document.NewUint64Value(10), true, nil},
		{"int", document.NewIntValue(10), true, nil},
		{"int8", document.NewInt8Value(10), true, nil},
		{"int16", document.NewInt16Value(10), true, nil},
		{"int32", document.NewInt32Value(10), true, nil},
		{"int64", document.NewInt64Value(10), true, nil},
		{"float64", document.NewFloat64Value(10), true, nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.ConvertToArray()
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
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
