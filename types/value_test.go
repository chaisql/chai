package types_test

import (
	"math"
	"testing"
	"time"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func TestValueString(t *testing.T) {
	tests := []struct {
		name     string
		value    types.Value
		expected string
	}{
		{"null", types.NewNullValue(), "NULL"},
		{"bytes", types.NewBlobValue([]byte("bar")), "[98 97 114]"},
		{"string", types.NewTextValue("bar"), "\"bar\""},
		{"bool", types.NewBoolValue(true), "true"},
		{"int", types.NewIntegerValue(10), "10"},
		{"double", types.NewDoubleValue(10.1), "10.1"},
		{"double with no decimal", types.NewDoubleValue(10), "10"},
		{"big double", types.NewDoubleValue(1e21), "1e+21"},
		{"document", types.NewDocumentValue(document.NewFieldBuffer().Add("a", types.NewIntegerValue(10))), "{\"a\": 10}"},
		{"array", types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(10))), "[10]"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, test.value.String())
		})
	}
}

func TestNewValue(t *testing.T) {
	type myBytes []byte
	type myString string
	type myUint uint
	type myUint16 uint16
	type myUint32 uint32
	type myUint64 uint64
	type myInt int
	type myInt8 int8
	type myInt16 int16
	type myInt64 int64
	type myFloat64 float64

	now := time.Now()

	tests := []struct {
		name            string
		value, expected interface{}
	}{
		{"bytes", []byte("bar"), []byte("bar")},
		{"string", "bar", "bar"},
		{"bool", true, true},
		{"uint", uint(10), int64(10)},
		{"uint8", uint8(10), int64(10)},
		{"uint16", uint16(10), int64(10)},
		{"uint32", uint32(10), int64(10)},
		{"uint64", uint64(10), int64(10)},
		{"int", int(10), int64(10)},
		{"int8", int8(10), int64(10)},
		{"int16", int16(10), int64(10)},
		{"int32", int32(10), int64(10)},
		{"int64", int64(10), int64(10)},
		{"float64", 10.1, float64(10.1)},
		{"null", nil, nil},
		{"document", document.NewFieldBuffer().Add("a", types.NewIntegerValue(10)), document.NewFieldBuffer().Add("a", types.NewIntegerValue(10))},
		{"array", document.NewValueBuffer(types.NewIntegerValue(10)), document.NewValueBuffer(types.NewIntegerValue(10))},
		{"time", now, now.Format(time.RFC3339Nano)},
		{"bytes", myBytes("bar"), []byte("bar")},
		{"string", myString("bar"), "bar"},
		{"myUint", myUint(10), int64(10)},
		{"myUint16", myUint16(500), int64(500)},
		{"myUint32", myUint32(90000), int64(90000)},
		{"myUint64", myUint64(100), int64(100)},
		{"myInt", myInt(7), int64(7)},
		{"myInt8", myInt8(3), int64(3)},
		{"myInt16", myInt16(500), int64(500)},
		{"myInt64", myInt64(10), int64(10)},
		{"myFloat64", myFloat64(10.1), float64(10.1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v, err := document.NewValue(test.value)
			require.NoError(t, err)
			require.Equal(t, test.expected, v.V())
		})
	}
}

func TestValueAdd(t *testing.T) {
	tests := []struct {
		name           string
		v, u, expected types.Value
		fails          bool
	}{
		{"null+null", types.NewNullValue(), types.NewNullValue(), types.NewNullValue(), false},
		{"null+integer(10)", types.NewNullValue(), types.NewIntegerValue(10), types.NewNullValue(), false},
		{"bool(true)+bool(true)", types.NewBoolValue(true), types.NewBoolValue(true), types.NewNullValue(), false},
		{"bool(true)+bool(false)", types.NewBoolValue(true), types.NewBoolValue(true), types.NewNullValue(), false},
		{"bool(true)+integer(-10)", types.NewBoolValue(true), types.NewIntegerValue(-10), types.NewNullValue(), false},
		{"integer(-10)+integer(10)", types.NewIntegerValue(-10), types.NewIntegerValue(10), types.NewIntegerValue(0), false},
		{"integer(120)+integer(120)", types.NewIntegerValue(120), types.NewIntegerValue(120), types.NewIntegerValue(240), false},
		{"integer(120)+float64(120)", types.NewIntegerValue(120), types.NewDoubleValue(120), types.NewDoubleValue(240), false},
		{"integer(120)+float64(120.1)", types.NewIntegerValue(120), types.NewDoubleValue(120.1), types.NewDoubleValue(240.1), false},
		{"int64(max)+integer(10)", types.NewIntegerValue(math.MaxInt64), types.NewIntegerValue(10), types.NewDoubleValue(math.MaxInt64 + 10), false},
		{"int64(min)+integer(-10)", types.NewIntegerValue(math.MinInt64), types.NewIntegerValue(-10), types.NewDoubleValue(math.MinInt64 - 10), false},
		{"integer(120)+text('120')", types.NewIntegerValue(120), types.NewTextValue("120"), types.NewNullValue(), false},
		{"text('120')+text('120')", types.NewTextValue("120"), types.NewTextValue("120"), types.NewNullValue(), false},
		{"document+document", types.NewDocumentValue(document.NewFieldBuffer().Add("a", types.NewIntegerValue(10))), types.NewDocumentValue(document.NewFieldBuffer().Add("a", types.NewIntegerValue(10))), types.NewNullValue(), false},
		{"array+array", types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(10))), types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(10))), types.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := types.Add(test.v, test.u)
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
		v, u, expected types.Value
		fails          bool
	}{
		{"null-null", types.NewNullValue(), types.NewNullValue(), types.NewNullValue(), false},
		{"null-integer(10)", types.NewNullValue(), types.NewIntegerValue(10), types.NewNullValue(), false},
		{"bool(true)-bool(true)", types.NewBoolValue(true), types.NewBoolValue(true), types.NewNullValue(), false},
		{"bool(true)-bool(false)", types.NewBoolValue(true), types.NewBoolValue(false), types.NewNullValue(), false},
		{"bool(true)-integer(-10)", types.NewBoolValue(true), types.NewIntegerValue(-10), types.NewNullValue(), false},
		{"integer(10)-integer(10)", types.NewIntegerValue(10), types.NewIntegerValue(10), types.NewIntegerValue(0), false},
		{"int16(250)-int16(220)", types.NewIntegerValue(250), types.NewIntegerValue(220), types.NewIntegerValue(30), false},
		{"integer(120)-float64(620)", types.NewIntegerValue(120), types.NewDoubleValue(620), types.NewDoubleValue(-500), false},
		{"integer(120)-float64(120.1)", types.NewIntegerValue(120), types.NewDoubleValue(120.1), types.NewDoubleValue(-0.09999999999999432), false},
		{"int64(min)-integer(10)", types.NewIntegerValue(math.MinInt64), types.NewIntegerValue(10), types.NewDoubleValue(math.MinInt64 - 10), false},
		{"int64(max)-integer(-10)", types.NewIntegerValue(math.MaxInt64), types.NewIntegerValue(-10), types.NewDoubleValue(math.MaxInt64 + 10), false},
		{"integer(120)-text('120')", types.NewIntegerValue(120), types.NewTextValue("120"), types.NewNullValue(), false},
		{"text('120')-text('120')", types.NewTextValue("120"), types.NewTextValue("120"), types.NewNullValue(), false},
		{"document-document", types.NewDocumentValue(document.NewFieldBuffer().Add("a", types.NewIntegerValue(10))), types.NewDocumentValue(document.NewFieldBuffer().Add("a", types.NewIntegerValue(10))), types.NewNullValue(), false},
		{"array-array", types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(10))), types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(10))), types.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := types.Sub(test.v, test.u)
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
		v, u, expected types.Value
		fails          bool
	}{
		{"null*null", types.NewNullValue(), types.NewNullValue(), types.NewNullValue(), false},
		{"null*integer(10)", types.NewNullValue(), types.NewIntegerValue(10), types.NewNullValue(), false},
		{"bool(true)*bool(true)", types.NewBoolValue(true), types.NewBoolValue(true), types.NewNullValue(), false},
		{"bool(true)*bool(false)", types.NewBoolValue(true), types.NewBoolValue(false), types.NewNullValue(), false},
		{"bool(true)*integer(-10)", types.NewBoolValue(true), types.NewIntegerValue(-10), types.NewNullValue(), false},
		{"integer(10)*integer(10)", types.NewIntegerValue(10), types.NewIntegerValue(10), types.NewIntegerValue(100), false},
		{"integer(10)*integer(80)", types.NewIntegerValue(10), types.NewIntegerValue(80), types.NewIntegerValue(800), false},
		{"integer(10)*float64(80)", types.NewIntegerValue(10), types.NewDoubleValue(80), types.NewDoubleValue(800), false},
		{"int64(max)*int64(max)", types.NewIntegerValue(math.MaxInt64), types.NewIntegerValue(math.MaxInt64), types.NewDoubleValue(math.MaxInt64 * math.MaxInt64), false},
		{"integer(120)*text('120')", types.NewIntegerValue(120), types.NewTextValue("120"), types.NewNullValue(), false},
		{"text('120')*text('120')", types.NewTextValue("120"), types.NewTextValue("120"), types.NewNullValue(), false},
		{"document*document", types.NewDocumentValue(document.NewFieldBuffer().Add("a", types.NewIntegerValue(10))), types.NewDocumentValue(document.NewFieldBuffer().Add("a", types.NewIntegerValue(10))), types.NewNullValue(), false},
		{"array*array", types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(10))), types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(10))), types.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := types.Mul(test.v, test.u)
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
		v, u, expected types.Value
		fails          bool
	}{
		{"null/null", types.NewNullValue(), types.NewNullValue(), types.NewNullValue(), false},
		{"null/integer(10)", types.NewNullValue(), types.NewIntegerValue(10), types.NewNullValue(), false},
		{"bool(true)/bool(true)", types.NewBoolValue(true), types.NewBoolValue(true), types.NewNullValue(), false},
		{"bool(true)/bool(false)", types.NewBoolValue(true), types.NewBoolValue(false), types.NewNullValue(), false},
		{"integer(10)/integer(0)", types.NewIntegerValue(10), types.NewIntegerValue(0), types.NewNullValue(), false},
		{"integer(10)/float64(0)", types.NewIntegerValue(10), types.NewDoubleValue(0), types.NewNullValue(), false},
		{"integer(10)/integer(10)", types.NewIntegerValue(10), types.NewIntegerValue(10), types.NewIntegerValue(1), false},
		{"integer(10)/integer(8)", types.NewIntegerValue(10), types.NewIntegerValue(8), types.NewIntegerValue(1), false},
		{"integer(10)/float64(8)", types.NewIntegerValue(10), types.NewDoubleValue(8), types.NewDoubleValue(1.25), false},
		{"int64(maxint)/float64(maxint)", types.NewIntegerValue(math.MaxInt64), types.NewDoubleValue(math.MaxInt64), types.NewDoubleValue(1), false},
		{"integer(120)/text('120')", types.NewIntegerValue(120), types.NewTextValue("120"), types.NewNullValue(), false},
		{"text('120')/text('120')", types.NewTextValue("120"), types.NewTextValue("120"), types.NewNullValue(), false},
		{"document/document", types.NewDocumentValue(document.NewFieldBuffer().Add("a", types.NewIntegerValue(10))), types.NewDocumentValue(document.NewFieldBuffer().Add("a", types.NewIntegerValue(10))), types.NewNullValue(), false},
		{"array/array", types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(10))), types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(10))), types.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := types.Div(test.v, test.u)
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
		v, u, expected types.Value
		fails          bool
	}{
		{"null%null", types.NewNullValue(), types.NewNullValue(), types.NewNullValue(), false},
		{"null%integer(10)", types.NewNullValue(), types.NewIntegerValue(10), types.NewNullValue(), false},
		{"bool(true)%bool(true)", types.NewBoolValue(true), types.NewBoolValue(true), types.NewNullValue(), false},
		{"bool(true)%bool(false)", types.NewBoolValue(true), types.NewBoolValue(false), types.NewNullValue(), false},
		{"integer(10)%integer(0)", types.NewIntegerValue(10), types.NewIntegerValue(0), types.NewNullValue(), false},
		{"integer(10)%float64(0)", types.NewIntegerValue(10), types.NewDoubleValue(0), types.NewNullValue(), false},
		{"integer(10)%integer(10)", types.NewIntegerValue(10), types.NewIntegerValue(10), types.NewIntegerValue(0), false},
		{"integer(10)%integer(8)", types.NewIntegerValue(10), types.NewIntegerValue(8), types.NewIntegerValue(2), false},
		{"integer(10)%float64(8)", types.NewIntegerValue(10), types.NewDoubleValue(8), types.NewDoubleValue(2), false},
		{"int64(maxint)%float64(maxint)", types.NewIntegerValue(math.MaxInt64), types.NewDoubleValue(math.MaxInt64), types.NewDoubleValue(0), false},
		{"double(> maxint)%int64(100)", types.NewDoubleValue(math.MaxInt64 + 1000), types.NewIntegerValue(100), types.NewDoubleValue(8), false},
		{"int64(100)%float64(> maxint)", types.NewIntegerValue(100), types.NewDoubleValue(math.MaxInt64 + 1000), types.NewDoubleValue(100), false},
		{"integer(120)%text('120')", types.NewIntegerValue(120), types.NewTextValue("120"), types.NewNullValue(), false},
		{"text('120')%text('120')", types.NewTextValue("120"), types.NewTextValue("120"), types.NewNullValue(), false},
		{"document%document", types.NewDocumentValue(document.NewFieldBuffer().Add("a", types.NewIntegerValue(10))), types.NewDocumentValue(document.NewFieldBuffer().Add("a", types.NewIntegerValue(10))), types.NewNullValue(), false},
		{"array%array", types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(10))), types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(10))), types.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := types.Mod(test.v, test.u)
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
		v, u, expected types.Value
		fails          bool
	}{
		{"null&null", types.NewNullValue(), types.NewNullValue(), types.NewNullValue(), false},
		{"null&integer(10)", types.NewNullValue(), types.NewIntegerValue(10), types.NewNullValue(), false},
		{"bool(true)&bool(true)", types.NewBoolValue(true), types.NewBoolValue(true), types.NewNullValue(), false},
		{"bool(true)&bool(false)", types.NewBoolValue(true), types.NewBoolValue(false), types.NewNullValue(), false},
		{"integer(10)&integer(0)", types.NewIntegerValue(10), types.NewIntegerValue(0), types.NewIntegerValue(0), false},
		{"double(10.5)&float64(3.2)", types.NewDoubleValue(10.5), types.NewDoubleValue(3.2), types.NewIntegerValue(2), false},
		{"integer(10)&float64(0)", types.NewIntegerValue(10), types.NewDoubleValue(0), types.NewIntegerValue(0), false},
		{"integer(10)&integer(10)", types.NewIntegerValue(10), types.NewIntegerValue(10), types.NewIntegerValue(10), false},
		{"integer(10)&integer(8)", types.NewIntegerValue(10), types.NewIntegerValue(8), types.NewIntegerValue(8), false},
		{"integer(10)&float64(8)", types.NewIntegerValue(10), types.NewDoubleValue(8), types.NewIntegerValue(8), false},
		{"text('120')&text('120')", types.NewTextValue("120"), types.NewTextValue("120"), types.NewNullValue(), false},
		{"document&document", types.NewDocumentValue(document.NewFieldBuffer().Add("a", types.NewIntegerValue(10))), types.NewDocumentValue(document.NewFieldBuffer().Add("a", types.NewIntegerValue(10))), types.NewNullValue(), false},
		{"array&array", types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(10))), types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(10))), types.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := types.BitwiseAnd(test.v, test.u)
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
		v, u, expected types.Value
		fails          bool
	}{
		{"null|null", types.NewNullValue(), types.NewNullValue(), types.NewNullValue(), false},
		{"null|integer(10)", types.NewNullValue(), types.NewIntegerValue(10), types.NewNullValue(), false},
		{"bool(true)|bool(true)", types.NewBoolValue(true), types.NewBoolValue(true), types.NewNullValue(), false},
		{"bool(true)|bool(false)", types.NewBoolValue(true), types.NewBoolValue(false), types.NewNullValue(), false},
		{"integer(10)|integer(0)", types.NewIntegerValue(10), types.NewIntegerValue(0), types.NewIntegerValue(10), false},
		{"double(10.5)|float64(3.2)", types.NewDoubleValue(10.5), types.NewDoubleValue(3.2), types.NewIntegerValue(11), false},
		{"integer(10)|float64(0)", types.NewIntegerValue(10), types.NewDoubleValue(0), types.NewIntegerValue(10), false},
		{"integer(10)|integer(10)", types.NewIntegerValue(10), types.NewIntegerValue(10), types.NewIntegerValue(10), false},
		{"integer(10)|float64(8)", types.NewIntegerValue(10), types.NewDoubleValue(8), types.NewIntegerValue(10), false},
		{"text('120')|text('120')", types.NewTextValue("120"), types.NewTextValue("120"), types.NewNullValue(), false},
		{"document|document", types.NewDocumentValue(document.NewFieldBuffer().Add("a", types.NewIntegerValue(10))), types.NewDocumentValue(document.NewFieldBuffer().Add("a", types.NewIntegerValue(10))), types.NewNullValue(), false},
		{"array|array", types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(10))), types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(10))), types.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := types.BitwiseOr(test.v, test.u)
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
		v, u, expected types.Value
		fails          bool
	}{
		{"null^null", types.NewNullValue(), types.NewNullValue(), types.NewNullValue(), false},
		{"null^integer(10)", types.NewNullValue(), types.NewIntegerValue(10), types.NewNullValue(), false},
		{"bool(true)^bool(true)", types.NewBoolValue(true), types.NewBoolValue(true), types.NewNullValue(), false},
		{"bool(true)^bool(false)", types.NewBoolValue(true), types.NewBoolValue(false), types.NewNullValue(), false},
		{"integer(10)^integer(0)", types.NewIntegerValue(10), types.NewIntegerValue(0), types.NewIntegerValue(10), false},
		{"double(10.5)^double(3.2)", types.NewDoubleValue(10.5), types.NewDoubleValue(3.2), types.NewIntegerValue(9), false},
		{"integer(10)^double(0)", types.NewIntegerValue(10), types.NewDoubleValue(0), types.NewIntegerValue(10), false},
		{"integer(10)^integer(10)", types.NewIntegerValue(10), types.NewIntegerValue(10), types.NewIntegerValue(0), false},
		{"text('120')^text('120')", types.NewTextValue("120"), types.NewTextValue("120"), types.NewNullValue(), false},
		{"document^document", types.NewDocumentValue(document.NewFieldBuffer().Add("a", types.NewIntegerValue(10))), types.NewDocumentValue(document.NewFieldBuffer().Add("a", types.NewIntegerValue(10))), types.NewNullValue(), false},
		{"array^array", types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(10))), types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(10))), types.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := types.BitwiseXor(test.v, test.u)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}
