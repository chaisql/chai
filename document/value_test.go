package document_test

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/genjidb/genji/document"
	"github.com/stretchr/testify/require"
)

func TestValueString(t *testing.T) {
	tests := []struct {
		name     string
		value    document.Value
		expected string
	}{
		{"null", document.NewNullValue(), "NULL"},
		{"bytes", document.NewBlobValue([]byte("bar")), "[98 97 114]"},
		{"string", document.NewTextValue("bar"), "bar"},
		{"bool", document.NewBoolValue(true), "true"},
		{"int", document.NewIntValue(10), "10"},
		{"int8", document.NewInt8Value(10), "10"},
		{"int16", document.NewInt16Value(10), "10"},
		{"int32", document.NewInt32Value(10), "10"},
		{"int64", document.NewInt64Value(10), "10"},
		{"float64", document.NewFloat64Value(10.1), "10.1"},
		{"document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntValue(10))), "{\"a\":10}\n"},
		{"array", document.NewArrayValue(document.NewValueBuffer(document.NewIntValue(10))), "[10]\n"},
		{"duration", document.NewDurationValue(10 * time.Nanosecond), "10ns"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, test.value.String())
		})
	}
}

func TestNewValue(t *testing.T) {
	type st struct {
		A int
		B string
	}
	type myBytes []byte
	type myString string
	type myBool bool
	type myUint uint
	type myUint16 uint16
	type myUint32 uint32
	type myUint64 uint64
	type myInt int
	type myInt8 int8
	type myInt16 int16
	type myInt64 int64
	type myFloat64 float64

	tests := []struct {
		name            string
		value, expected interface{}
	}{
		{"bytes", []byte("bar"), []byte("bar")},
		{"string", "bar", []byte("bar")},
		{"bool", true, true},
		{"uint", uint(10), int8(10)},
		{"uint8", uint8(10), int8(10)},
		{"uint16", uint16(10), int8(10)},
		{"uint16 big", uint16(500), int16(500)},
		{"uint32", uint32(10), int8(10)},
		{"uint64", uint64(10), int8(10)},
		{"int", int(10), int8(10)},
		{"int8", int8(10), int8(10)},
		{"int16", int16(10), int8(10)},
		{"int32", int32(10), int8(10)},
		{"int64", int64(10), int8(10)},
		{"float64", 10.1, float64(10.1)},
		{"null", nil, nil},
		{"document", document.NewFieldBuffer().Add("a", document.NewIntValue(10)), document.NewFieldBuffer().Add("a", document.NewIntValue(10))},
		{"array", document.NewValueBuffer(document.NewIntValue(10)), document.NewValueBuffer(document.NewIntValue(10))},
		{"duration", 10 * time.Nanosecond, 10 * time.Nanosecond},
		{"bytes", myBytes("bar"), []byte("bar")},
		{"string", myString("bar"), []byte("bar")},
		{"myUint", myUint(10), int8(10)},
		{"myUint16", myUint16(500), int16(500)},
		{"myUint32", myUint32(90000), int32(90000)},
		{"myUint64", myUint64(100), int8(100)},
		{"myInt", myInt(7), int8(7)},
		{"myInt8", myInt8(3), int8(3)},
		{"myInt16", myInt16(500), int16(500)},
		{"myInt64", myInt64(10), int8(10)},
		{"myFloat64", myFloat64(10.1), float64(10.1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v, err := document.NewValue(test.value)
			require.NoError(t, err)
			require.Equal(t, test.expected, v.V)
		})
	}
}

func TestConvertToBlob(t *testing.T) {
	tests := []struct {
		name     string
		v        document.Value
		fails    bool
		expected interface{}
	}{
		{"null", document.NewNullValue(), false, nil},
		{"bytes", document.NewBlobValue([]byte("bar")), false, []byte("bar")},
		{"string", document.NewTextValue("bar"), false, []byte("bar")},
		{"bool", document.NewBoolValue(true), true, nil},
		{"int", document.NewIntValue(10), true, nil},
		{"int8", document.NewInt8Value(10), true, nil},
		{"int16", document.NewInt16Value(10), true, nil},
		{"int32", document.NewInt32Value(10), true, nil},
		{"int64", document.NewInt64Value(10), true, nil},
		{"float64", document.NewFloat64Value(10.1), true, nil},
		{"document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntValue(10))), true, nil},
		{"array", document.NewArrayValue(document.NewValueBuffer(document.NewIntValue(10))), true, nil},
		{"duration", document.NewDurationValue(10 * time.Nanosecond), true, nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.ConvertTo(document.BlobValue)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res.V)
			}
		})
	}
}

func TestConvertToText(t *testing.T) {
	tests := []struct {
		name     string
		v        document.Value
		fails    bool
		expected interface{}
	}{
		{"null", document.NewNullValue(), false, nil},
		{"bytes", document.NewBlobValue([]byte("bar")), false, []byte("bar")},
		{"string", document.NewTextValue("bar"), false, []byte("bar")},
		{"bool", document.NewBoolValue(true), true, []byte{}},
		{"int", document.NewIntValue(10), true, []byte{}},
		{"int8", document.NewInt8Value(10), true, []byte{}},
		{"int16", document.NewInt16Value(10), true, []byte{}},
		{"int32", document.NewInt32Value(10), true, []byte{}},
		{"int64", document.NewInt64Value(10), true, []byte{}},
		{"float64", document.NewFloat64Value(10.1), true, []byte{}},
		{"document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntValue(10))), true, []byte{}},
		{"array", document.NewArrayValue(document.NewValueBuffer(document.NewIntValue(10))), true, []byte{}},
		{"duration", document.NewDurationValue(10 * time.Nanosecond), true, []byte{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.ConvertTo(document.TextValue)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res.V)
			}
		})
	}
}

func TestConvertToBool(t *testing.T) {
	tests := []struct {
		name     string
		v        document.Value
		fails    bool
		expected interface{}
	}{
		{"null", document.NewNullValue(), false, nil},
		{"bytes", document.NewBlobValue([]byte("bar")), false, true},
		{"zero bytes", document.NewBlobValue([]byte("")), false, false},
		{"string", document.NewTextValue("bar"), false, true},
		{"zero string", document.NewTextValue(""), false, false},
		{"bool", document.NewBoolValue(true), false, true},
		{"zero bool", document.NewBoolValue(false), false, false},
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
		{"document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewBoolValue(true))), false, true},
		{"zero document", document.NewDocumentValue(document.NewFieldBuffer()), false, true},
		{"array", document.NewArrayValue(document.NewValueBuffer(document.NewInt16Value(1))), false, true},
		{"zero array", document.NewArrayValue(document.NewValueBuffer()), false, true},
		{"duration", document.NewDurationValue(10 * time.Nanosecond), false, true},
		{"zero duration", document.NewDurationValue(0), false, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.ConvertTo(document.BoolValue)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res.V)
			}
		})
	}
}

func TestConvertToNumber(t *testing.T) {
	tests := []struct {
		name     string
		v        document.Value
		fails    bool
		expected interface{}
	}{
		{"null", document.NewNullValue(), false, nil},
		{"bytes", document.NewBlobValue([]byte("bar")), true, 0},
		{"string", document.NewTextValue("bar"), true, 0},
		{"bool", document.NewBoolValue(true), false, 1},
		{"int", document.NewIntValue(10), false, 10},
		{"int8", document.NewInt8Value(10), false, 10},
		{"int16", document.NewInt16Value(10), false, 10},
		{"int32", document.NewInt32Value(10), false, 10},
		{"int64", document.NewInt64Value(10), false, 10},
		{"float64", document.NewFloat64Value(10), false, 10},
		{"document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntValue(10))), true, 0},
		{"array", document.NewArrayValue(document.NewValueBuffer(document.NewIntValue(10))), true, 0},
		{"duration", document.NewDurationValue(10 * time.Nanosecond), false, 10},
	}

	check := func(t *testing.T, res document.Value, err error, fails bool, expected interface{}) {
		if fails {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, expected, res.V)
		}
	}

	for _, test := range tests {
		t.Run(test.name+" to int64", func(t *testing.T) {
			res, err := test.v.ConvertTo(document.Int64Value)
			expected := test.expected
			if expected != nil {
				expected = int64(expected.(int))
			}
			check(t, res, err, test.fails, expected)
		})
		t.Run(test.name+" to float64", func(t *testing.T) {
			res, err := test.v.ConvertTo(document.Float64Value)
			expected := test.expected
			if expected != nil {
				expected = float64(expected.(int))
			}
			check(t, res, err, test.fails, expected)
		})
	}

	t.Run("float64/precision loss", func(t *testing.T) {
		_, err := document.NewFloat64Value(10.4).ConvertTo(document.Int64Value)
		require.Error(t, err)
		_, err = document.NewFloat64Value(10.4).ConvertTo(document.Int32Value)
		require.Error(t, err)
	})

	t.Run("ints/overflow", func(t *testing.T) {
		tests := []struct {
			from, to document.ValueType
			x        interface{}
		}{
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

func TestConvertToDuration(t *testing.T) {
	tests := []struct {
		name     string
		v        document.Value
		fails    bool
		expected interface{}
	}{
		{"null", document.NewNullValue(), false, nil},
		{"bytes", document.NewBlobValue([]byte("bar")), true, 0},
		{"string", document.NewTextValue("1ms"), false, time.Millisecond},
		{"bad string", document.NewTextValue("foo"), true, 0},
		{"bool", document.NewBoolValue(true), false, time.Nanosecond},
		{"int", document.NewIntValue(10), false, 10 * time.Nanosecond},
		{"int8", document.NewInt8Value(10), false, 10 * time.Nanosecond},
		{"int16", document.NewInt16Value(10), false, 10 * time.Nanosecond},
		{"int32", document.NewInt32Value(10), false, 10 * time.Nanosecond},
		{"int64", document.NewInt64Value(10), false, 10 * time.Nanosecond},
		{"float64", document.NewFloat64Value(10), false, 10 * time.Nanosecond},
		{"document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntValue(10))), true, 0},
		{"array", document.NewArrayValue(document.NewValueBuffer(document.NewIntValue(10))), true, 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.ConvertTo(document.DurationValue)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res.V)
			}
		})
	}
}

func TestConvertToDocument(t *testing.T) {
	tests := []struct {
		name     string
		v        document.Value
		fails    bool
		expected document.Value
	}{
		{"null", document.NewNullValue(), false, document.NewNullValue()},
		{"document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewInt16Value(10))), false, document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewInt16Value(10)))},
		{"bytes", document.NewBlobValue([]byte("bar")), true, document.Value{}},
		{"string", document.NewTextValue("bar"), true, document.Value{}},
		{"bool", document.NewBoolValue(true), true, document.Value{}},
		{"int", document.NewIntValue(10), true, document.Value{}},
		{"int8", document.NewInt8Value(10), true, document.Value{}},
		{"int16", document.NewInt16Value(10), true, document.Value{}},
		{"int32", document.NewInt32Value(10), true, document.Value{}},
		{"int64", document.NewInt64Value(10), true, document.Value{}},
		{"float64", document.NewFloat64Value(10), true, document.Value{}},
		{"array", document.NewArrayValue(document.NewValueBuffer(document.NewIntValue(10))), true, document.Value{}},
		{"duration", document.NewDurationValue(10 * time.Nanosecond), true, document.Value{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.ConvertTo(document.DocumentValue)
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
		{"array", document.NewArrayValue(document.NewValueBuffer().Append(document.NewInt16Value(10))), false, document.NewValueBuffer().Append(document.NewInt16Value(10))},
		{"bytes", document.NewBlobValue([]byte("bar")), true, nil},
		{"string", document.NewTextValue("bar"), true, nil},
		{"bool", document.NewBoolValue(true), true, nil},
		{"int", document.NewIntValue(10), true, nil},
		{"int8", document.NewInt8Value(10), true, nil},
		{"int16", document.NewInt16Value(10), true, nil},
		{"int32", document.NewInt32Value(10), true, nil},
		{"int64", document.NewInt64Value(10), true, nil},
		{"float64", document.NewFloat64Value(10), true, nil},
		{"document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntValue(10))), true, nil},
		{"duration", document.NewDurationValue(10 * time.Nanosecond), true, nil},
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

func TestValueAdd(t *testing.T) {
	tests := []struct {
		name           string
		v, u, expected document.Value
		fails          bool
	}{
		{"null+null", document.NewNullValue(), document.NewNullValue(), document.NewNullValue(), false},
		{"null+int8(10)", document.NewNullValue(), document.NewInt8Value(10), document.NewNullValue(), false},
		{"bool(true)+bool(true)", document.NewBoolValue(true), document.NewBoolValue(true), document.NewInt8Value(2), false},
		{"bool(true)+bool(false)", document.NewBoolValue(true), document.NewBoolValue(true), document.NewInt8Value(2), false},
		{"bool(true)+int8(-10)", document.NewBoolValue(true), document.NewInt8Value(-10), document.NewInt8Value(-9), false},
		{"int8(-10)+int8(10)", document.NewInt8Value(-10), document.NewInt8Value(10), document.NewInt8Value(0), false},
		{"int8(120)+int8(120)", document.NewInt8Value(120), document.NewInt8Value(120), document.NewInt16Value(240), false},
		{"int8(120)+float64(120)", document.NewInt8Value(120), document.NewFloat64Value(120), document.NewFloat64Value(240), false},
		{"int8(120)+float64(120.1)", document.NewInt8Value(120), document.NewFloat64Value(120.1), document.NewFloat64Value(240.1), false},
		{"int64(max)+int8(10)", document.NewInt64Value(math.MaxInt64), document.NewIntValue(10), document.NewFloat64Value(math.MaxInt64 + 10), false},
		{"int64(min)+int8(-10)", document.NewInt64Value(math.MinInt64), document.NewIntValue(-10), document.NewFloat64Value(math.MinInt64 - 10), false},
		{"int8(120)+text('120')", document.NewInt8Value(120), document.NewTextValue("120"), document.NewNullValue(), false},
		{"text('120')+text('120')", document.NewTextValue("120"), document.NewTextValue("120"), document.Value{}, true},
		{"document+document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntValue(10))), document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntValue(10))), document.Value{}, true},
		{"array+array", document.NewArrayValue(document.NewValueBuffer(document.NewIntValue(10))), document.NewArrayValue(document.NewValueBuffer(document.NewIntValue(10))), document.Value{}, true},
		{"duration(1ns)+duration(1ms)", document.NewDurationValue(time.Nanosecond), document.NewDurationValue(time.Millisecond), document.NewDurationValue(time.Nanosecond + time.Millisecond), false},
		{"int8(1)+text('foo')", document.NewInt8Value(1), document.NewTextValue("foo"), document.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.Add(test.u)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestValueSub(t *testing.T) {
	tests := []struct {
		name           string
		v, u, expected document.Value
		fails          bool
	}{
		{"null-null", document.NewNullValue(), document.NewNullValue(), document.NewNullValue(), false},
		{"null-int8(10)", document.NewNullValue(), document.NewInt8Value(10), document.NewNullValue(), false},
		{"bool(true)-bool(true)", document.NewBoolValue(true), document.NewBoolValue(true), document.NewInt8Value(0), false},
		{"bool(true)-bool(false)", document.NewBoolValue(true), document.NewBoolValue(false), document.NewInt8Value(1), false},
		{"bool(true)-int8(-10)", document.NewBoolValue(true), document.NewInt8Value(-10), document.NewInt8Value(11), false},
		{"int8(10)-int8(10)", document.NewInt8Value(10), document.NewInt8Value(10), document.NewInt8Value(0), false},
		{"int16(250)-int16(220)", document.NewInt16Value(250), document.NewInt16Value(220), document.NewInt8Value(30), false},
		{"int8(120)-float64(620)", document.NewInt8Value(120), document.NewFloat64Value(620), document.NewFloat64Value(-500), false},
		{"int8(120)-float64(120.1)", document.NewInt8Value(120), document.NewFloat64Value(120.1), document.NewFloat64Value(-0.09999999999999432), false},
		{"int64(min)-int8(10)", document.NewInt64Value(math.MinInt64), document.NewIntValue(10), document.NewFloat64Value(math.MinInt64 - 10), false},
		{"int64(max)-int8(-10)", document.NewInt64Value(math.MaxInt64), document.NewIntValue(-10), document.NewFloat64Value(math.MaxInt64 + 10), false},
		{"int8(120)-text('120')", document.NewInt8Value(120), document.NewTextValue("120"), document.NewNullValue(), false},
		{"text('120')-text('120')", document.NewTextValue("120"), document.NewTextValue("120"), document.Value{}, true},
		{"document-document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntValue(10))), document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntValue(10))), document.Value{}, true},
		{"array-array", document.NewArrayValue(document.NewValueBuffer(document.NewIntValue(10))), document.NewArrayValue(document.NewValueBuffer(document.NewIntValue(10))), document.Value{}, true},
		{"duration(1ns)-duration(1ms)", document.NewDurationValue(time.Nanosecond), document.NewDurationValue(time.Millisecond), document.NewDurationValue(time.Nanosecond - time.Millisecond), false},
		{"int8(1)-text('foo')", document.NewInt8Value(1), document.NewTextValue("foo"), document.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.Sub(test.u)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestValueMult(t *testing.T) {
	tests := []struct {
		name           string
		v, u, expected document.Value
		fails          bool
	}{
		{"null*null", document.NewNullValue(), document.NewNullValue(), document.NewNullValue(), false},
		{"null*int8(10)", document.NewNullValue(), document.NewInt8Value(10), document.NewNullValue(), false},
		{"bool(true)*bool(true)", document.NewBoolValue(true), document.NewBoolValue(true), document.NewInt8Value(1), false},
		{"bool(true)*bool(false)", document.NewBoolValue(true), document.NewBoolValue(false), document.NewInt8Value(0), false},
		{"bool(true)*int8(-10)", document.NewBoolValue(true), document.NewInt8Value(-10), document.NewInt8Value(-10), false},
		{"int8(10)*int8(10)", document.NewInt8Value(10), document.NewInt8Value(10), document.NewInt8Value(100), false},
		{"int8(10)*int8(80)", document.NewInt8Value(10), document.NewInt8Value(80), document.NewInt16Value(800), false},
		{"int8(10)*float64(80)", document.NewInt8Value(10), document.NewFloat64Value(80), document.NewFloat64Value(800), false},
		{"int64(max)*int64(max)", document.NewInt64Value(math.MaxInt64), document.NewInt64Value(math.MaxInt64), document.NewFloat64Value(math.MaxInt64 * math.MaxInt64), false},
		{"int8(120)*text('120')", document.NewInt8Value(120), document.NewTextValue("120"), document.NewNullValue(), false},
		{"text('120')*text('120')", document.NewTextValue("120"), document.NewTextValue("120"), document.Value{}, true},
		{"document*document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntValue(10))), document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntValue(10))), document.Value{}, true},
		{"array*array", document.NewArrayValue(document.NewValueBuffer(document.NewIntValue(10))), document.NewArrayValue(document.NewValueBuffer(document.NewIntValue(10))), document.Value{}, true},
		{"duration(10ns)*duration(1ms)", document.NewDurationValue(10 * time.Nanosecond), document.NewDurationValue(time.Millisecond), document.NewDurationValue(10 * time.Nanosecond * time.Millisecond), false},
		{"int8(1)*text('foo')", document.NewInt8Value(1), document.NewTextValue("foo"), document.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.Mul(test.u)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestValueDiv(t *testing.T) {
	tests := []struct {
		name           string
		v, u, expected document.Value
		fails          bool
	}{
		{"null/null", document.NewNullValue(), document.NewNullValue(), document.NewNullValue(), false},
		{"null/int8(10)", document.NewNullValue(), document.NewInt8Value(10), document.NewNullValue(), false},
		{"bool(true)/bool(true)", document.NewBoolValue(true), document.NewBoolValue(true), document.NewInt8Value(1), false},
		{"bool(true)/bool(false)", document.NewBoolValue(true), document.NewBoolValue(false), document.NewNullValue(), false},
		{"int8(10)/int8(0)", document.NewInt8Value(10), document.NewInt8Value(0), document.NewNullValue(), false},
		{"int8(10)/float64(0)", document.NewInt8Value(10), document.NewFloat64Value(0), document.NewNullValue(), false},
		{"int8(10)/int8(10)", document.NewInt8Value(10), document.NewInt8Value(10), document.NewInt8Value(1), false},
		{"int8(10)/int8(8)", document.NewInt8Value(10), document.NewInt8Value(8), document.NewInt8Value(1), false},
		{"int8(10)/float64(8)", document.NewInt8Value(10), document.NewFloat64Value(8), document.NewFloat64Value(1.25), false},
		{"int64(maxint)/float64(maxint)", document.NewInt64Value(math.MaxInt64), document.NewFloat64Value(math.MaxInt64), document.NewFloat64Value(1), false},
		{"int8(120)/text('120')", document.NewInt8Value(120), document.NewTextValue("120"), document.NewNullValue(), false},
		{"text('120')/text('120')", document.NewTextValue("120"), document.NewTextValue("120"), document.Value{}, true},
		{"document/document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntValue(10))), document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntValue(10))), document.Value{}, true},
		{"array/array", document.NewArrayValue(document.NewValueBuffer(document.NewIntValue(10))), document.NewArrayValue(document.NewValueBuffer(document.NewIntValue(10))), document.Value{}, true},
		{"duration(10ns)/duration(1ms)", document.NewDurationValue(10 * time.Nanosecond), document.NewDurationValue(time.Millisecond), document.NewDurationValue(10 * time.Nanosecond / time.Millisecond), false},
		{"int8(1)/text('foo')", document.NewInt8Value(1), document.NewTextValue("foo"), document.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.Div(test.u)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestValueMod(t *testing.T) {
	tests := []struct {
		name           string
		v, u, expected document.Value
		fails          bool
	}{
		{"null%null", document.NewNullValue(), document.NewNullValue(), document.NewNullValue(), false},
		{"null%int8(10)", document.NewNullValue(), document.NewInt8Value(10), document.NewNullValue(), false},
		{"bool(true)%bool(true)", document.NewBoolValue(true), document.NewBoolValue(true), document.NewInt8Value(0), false},
		{"bool(true)%bool(false)", document.NewBoolValue(true), document.NewBoolValue(false), document.NewNullValue(), false},
		{"int8(10)%int8(0)", document.NewInt8Value(10), document.NewInt8Value(0), document.NewNullValue(), false},
		{"int8(10)%float64(0)", document.NewInt8Value(10), document.NewFloat64Value(0), document.NewNullValue(), false},
		{"int8(10)%int8(10)", document.NewInt8Value(10), document.NewInt8Value(10), document.NewInt8Value(0), false},
		{"int8(10)%int8(8)", document.NewInt8Value(10), document.NewInt8Value(8), document.NewInt8Value(2), false},
		{"int8(10)%float64(8)", document.NewInt8Value(10), document.NewFloat64Value(8), document.NewFloat64Value(2), false},
		{"int64(maxint)%float64(maxint)", document.NewInt64Value(math.MaxInt64), document.NewFloat64Value(math.MaxInt64), document.NewFloat64Value(0), false},
		{"float64(> maxint)%int64(100)", document.NewFloat64Value(math.MaxInt64 + 1000), document.NewInt8Value(100), document.NewFloat64Value(-8), false},
		{"int64(100)%float64(> maxint)", document.NewInt8Value(100), document.NewFloat64Value(math.MaxInt64 + 1000), document.NewFloat64Value(100), false},
		{"int8(120)%text('120')", document.NewInt8Value(120), document.NewTextValue("120"), document.NewNullValue(), false},
		{"text('120')%text('120')", document.NewTextValue("120"), document.NewTextValue("120"), document.Value{}, true},
		{"document%document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntValue(10))), document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntValue(10))), document.Value{}, true},
		{"array%array", document.NewArrayValue(document.NewValueBuffer(document.NewIntValue(10))), document.NewArrayValue(document.NewValueBuffer(document.NewIntValue(10))), document.Value{}, true},
		{"duration(10ns)%duration(1ms)", document.NewDurationValue(10 * time.Nanosecond), document.NewDurationValue(time.Millisecond), document.NewDurationValue(10 * time.Nanosecond % time.Millisecond), false},
		{"int8(1)%text('foo')", document.NewInt8Value(1), document.NewTextValue("foo"), document.NewNullValue(), false},

	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.Mod(test.u)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestValueBitwiseAnd(t *testing.T) {
	tests := []struct {
		name           string
		v, u, expected document.Value
		fails          bool
	}{
		{"null&null", document.NewNullValue(), document.NewNullValue(), document.NewNullValue(), false},
		{"null&int8(10)", document.NewNullValue(), document.NewInt8Value(10), document.NewNullValue(), false},
		{"bool(true)&bool(true)", document.NewBoolValue(true), document.NewBoolValue(true), document.NewInt8Value(1), false},
		{"bool(true)&bool(false)", document.NewBoolValue(true), document.NewBoolValue(false), document.NewInt8Value(0), false},
		{"int8(10)&int8(0)", document.NewInt8Value(10), document.NewInt8Value(0), document.NewInt8Value(0), false},
		{"float64(10.5)&float64(3.2)", document.NewFloat64Value(10.5), document.NewFloat64Value(3.2), document.NewInt8Value(2), false},
		{"int8(10)&float64(0)", document.NewInt8Value(10), document.NewFloat64Value(0), document.NewInt8Value(0), false},
		{"int8(10)&int8(10)", document.NewInt8Value(10), document.NewInt8Value(10), document.NewInt8Value(10), false},
		{"int8(10)&int8(8)", document.NewInt8Value(10), document.NewInt8Value(8), document.NewInt8Value(8), false},
		{"int8(10)&float64(8)", document.NewInt8Value(10), document.NewFloat64Value(8), document.NewInt8Value(8), false},
		{"text('120')&text('120')", document.NewTextValue("120"), document.NewTextValue("120"), document.Value{}, true},
		{"document&document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntValue(10))), document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntValue(10))), document.Value{}, true},
		{"array&array", document.NewArrayValue(document.NewValueBuffer(document.NewIntValue(10))), document.NewArrayValue(document.NewValueBuffer(document.NewIntValue(10))), document.Value{}, true},
		{"duration(10ns)&duration(1ms)", document.NewDurationValue(10 * time.Nanosecond), document.NewDurationValue(time.Microsecond), document.NewIntValue(8), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.BitwiseAnd(test.u)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestValueBitwiseOr(t *testing.T) {
	tests := []struct {
		name           string
		v, u, expected document.Value
		fails          bool
	}{
		{"null|null", document.NewNullValue(), document.NewNullValue(), document.NewNullValue(), false},
		{"null|int8(10)", document.NewNullValue(), document.NewInt8Value(10), document.NewNullValue(), false},
		{"bool(true)|bool(true)", document.NewBoolValue(true), document.NewBoolValue(true), document.NewInt8Value(1), false},
		{"bool(true)|bool(false)", document.NewBoolValue(true), document.NewBoolValue(false), document.NewInt8Value(1), false},
		{"int8(10)|int8(0)", document.NewInt8Value(10), document.NewInt8Value(0), document.NewInt8Value(10), false},
		{"float64(10.5)|float64(3.2)", document.NewFloat64Value(10.5), document.NewFloat64Value(3.2), document.NewInt8Value(11), false},
		{"int8(10)|float64(0)", document.NewInt8Value(10), document.NewFloat64Value(0), document.NewInt8Value(10), false},
		{"int8(10)|int8(10)", document.NewInt8Value(10), document.NewInt8Value(10), document.NewInt8Value(10), false},
		{"int8(10)|float64(8)", document.NewInt8Value(10), document.NewFloat64Value(8), document.NewInt8Value(10), false},
		{"text('120')|text('120')", document.NewTextValue("120"), document.NewTextValue("120"), document.Value{}, true},
		{"document|document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntValue(10))), document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntValue(10))), document.Value{}, true},
		{"array|array", document.NewArrayValue(document.NewValueBuffer(document.NewIntValue(10))), document.NewArrayValue(document.NewValueBuffer(document.NewIntValue(10))), document.Value{}, true},
		{"duration(10ns)|duration(1ms)", document.NewDurationValue(10 * time.Nanosecond), document.NewDurationValue(time.Microsecond), document.NewIntValue(1002), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.BitwiseOr(test.u)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestValueBitwiseXor(t *testing.T) {
	tests := []struct {
		name           string
		v, u, expected document.Value
		fails          bool
	}{
		{"null^null", document.NewNullValue(), document.NewNullValue(), document.NewNullValue(), false},
		{"null^int8(10)", document.NewNullValue(), document.NewInt8Value(10), document.NewNullValue(), false},
		{"bool(true)^bool(true)", document.NewBoolValue(true), document.NewBoolValue(true), document.NewInt8Value(0), false},
		{"bool(true)^bool(false)", document.NewBoolValue(true), document.NewBoolValue(false), document.NewInt8Value(1), false},
		{"int8(10)^int8(0)", document.NewInt8Value(10), document.NewInt8Value(0), document.NewInt8Value(10), false},
		{"float64(10.5)^float64(3.2)", document.NewFloat64Value(10.5), document.NewFloat64Value(3.2), document.NewInt8Value(9), false},
		{"int8(10)^float64(0)", document.NewInt8Value(10), document.NewFloat64Value(0), document.NewInt8Value(10), false},
		{"int8(10)^int8(10)", document.NewInt8Value(10), document.NewInt8Value(10), document.NewInt8Value(0), false},
		{"text('120')^text('120')", document.NewTextValue("120"), document.NewTextValue("120"), document.Value{}, true},
		{"document^document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntValue(10))), document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntValue(10))), document.Value{}, true},
		{"array^array", document.NewArrayValue(document.NewValueBuffer(document.NewIntValue(10))), document.NewArrayValue(document.NewValueBuffer(document.NewIntValue(10))), document.Value{}, true},
		{"duration(10ns)^duration(1ms)", document.NewDurationValue(10 * time.Nanosecond), document.NewDurationValue(time.Microsecond), document.NewIntValue(994), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.BitwiseXor(test.u)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

type CompareTest struct {
	name     string
	v, u     document.Value
	expected int
}

func TestValueCompare(t *testing.T) {
	tests := []CompareTest{
		{"null,null", document.NewNullValue(), document.NewNullValue(), 0},
		{"null,int8", document.NewNullValue(), document.NewInt8Value(0), -1},
		{"int8,null", document.NewInt8Value(0), document.NewNullValue(), 1},
	}

	// cartesian computes a cartesian product, generating all possible combinations of the passed arrays
	cartesian := func(vals ...[]document.Value) {
		for _, x := range vals {
			for _, y := range vals {
				for i := 0; i < 2; i++ {
					for j := 0; j < 2; j++ {
						if j == 1 && i == 1 {
							continue
						}
						v := x[i]
						u := y[j]
						signum := int((int64(i-j) >> 63) | int64(uint64(j-i)>>63))
						tests = append(tests, CompareTest{
							name:     fmt.Sprintf("%s(%s)%s%s(%s)", v.Type, v, []string{"<", "=", ">"}[signum+1], u.Type, u),
							v:        v,
							u:        u,
							expected: signum,
						})
					}
				}
			}
		}
	}

	// sample numeric values. Values at index [0] are known to be less than values at index [1]
	int8s := []document.Value{document.NewInt8Value(0), document.NewInt8Value(1)}
	int16s := []document.Value{document.NewInt16Value(0), document.NewInt16Value(1)}
	int32s := []document.Value{document.NewInt32Value(0), document.NewInt32Value(1)}
	int64s := []document.Value{document.NewInt64Value(0), document.NewInt64Value(1)}
	float64s := []document.Value{document.NewFloat64Value(0), document.NewFloat64Value(1)}
	bools := []document.Value{document.NewBoolValue(false), document.NewBoolValue(true)}
	texts := []document.Value{document.NewTextValue("0"), document.NewTextValue("1")}

	// generate a batch of tests mixing everything with everything
	cartesian(int8s, int16s, int32s, int64s, float64s, bools, texts)

	// Sample blob and text values. Values at index [0] are known to be less than values at index [1]
	texts = []document.Value{document.NewTextValue("ABC"), document.NewTextValue("CDE")}
	blobs := []document.Value{document.NewBlobValue([]byte{65, 66, 67}), document.NewBlobValue([]byte{68, 69, 70})}

	// generate another batch of tests mixing everything with everything
	cartesian(texts, blobs)

	// Run the tests
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res := test.v.Compare(test.u)
			require.Equal(t, test.expected, int((int64(res)>>63)|int64(uint64(-res)>>63)))
		})
	}
}
