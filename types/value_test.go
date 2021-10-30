package types_test

import (
	"math"
	"testing"
	"time"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

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
			assert.NoError(t, err)
			require.Equal(t, test.expected, v.V())
		})
	}
}

func TestValueMarshalText(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{"bytes", []byte("bar"), `"\x626172"`},
		{"string", "bar", `"bar"`},
		{"bool", true, "true"},
		{"int", int64(10), "10"},
		{"float64", 10.0, "10.0"},
		{"float64", 10.1, "10.1"},
		{"float64", math.MaxFloat64, "1.7976931348623157e+308"},
		{"null", nil, "NULL"},
		{"document", document.NewFieldBuffer().
			Add("a", types.NewIntegerValue(10)).
			Add("b c", types.NewTextValue("foo")).
			Add(`"d e"`, types.NewTextValue("foo")),
			"{a: 10, \"b c\": \"foo\", `\"d e\"`: \"foo\"}",
		},
		{"array", document.NewValueBuffer(types.NewIntegerValue(10), types.NewTextValue("foo")), `[10, "foo"]`},
		{"time", now, `"` + now.Format(time.RFC3339Nano) + `"`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v, err := document.NewValue(test.value)
			assert.NoError(t, err)
			data, err := v.MarshalText()
			assert.NoError(t, err)
			require.Equal(t, test.expected, string(data))
			if test.name != "time" {
				e := testutil.ParseExpr(t, string(data))
				got, err := e.Eval(&environment.Environment{})
				assert.NoError(t, err)
				require.Equal(t, test.value, got.V())
			}
		})
	}
}

func TestMarshalTextIndent(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{"bytes", []byte("bar"), `"\x626172"`},
		{"string", "bar", `"bar"`},
		{"bool", true, "true"},
		{"int", int64(10), "10"},
		{"float64", 10.0, "10.0"},
		{"float64", 10.1, "10.1"},
		{"float64", math.MaxFloat64, "1.7976931348623157e+308"},
		{"null", nil, "NULL"},
		{"document",
			document.NewFieldBuffer().Add("a", types.NewIntegerValue(10)).Add("b c", types.NewTextValue("foo")).Add("d", types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(10), types.NewTextValue("foo")))),
			`{
  a: 10,
  "b c": "foo",
  d: [
    10,
    "foo"
  ]
}`},
		{"array",
			document.NewValueBuffer(types.NewIntegerValue(10), types.NewTextValue("foo")),
			`[
  10,
  "foo"
]`,
		},
		{"time", now, `"` + now.Format(time.RFC3339Nano) + `"`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v, err := document.NewValue(test.value)
			assert.NoError(t, err)
			data, err := types.MarshalTextIndent(v, "\n", "  ")
			assert.NoError(t, err)
			require.Equal(t, test.expected, string(data))
			if test.name != "time" {
				e := testutil.ParseExpr(t, string(data))
				got, err := e.Eval(&environment.Environment{})
				assert.NoError(t, err)
				require.Equal(t, test.value, got.V())
			}
		})
	}
}

func TestValueMarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		value    types.Value
		expected string
	}{
		{"null", types.NewNullValue(), "null"},
		{"blob", types.NewBlobValue([]byte("bar")), `"YmFy"`},
		{"string", types.NewTextValue("bar"), `"bar"`},
		{"bool", types.NewBoolValue(true), "true"},
		{"int", types.NewIntegerValue(10), "10"},
		{"double", types.NewDoubleValue(10.1), "10.1"},
		{"double with no decimal", types.NewDoubleValue(10), "10"},
		{"big double", types.NewDoubleValue(1e15), "1e+15"},
		{"document", types.NewDocumentValue(document.NewFieldBuffer().Add("a", types.NewIntegerValue(10))), "{\"a\": 10}"},
		{"array", types.NewArrayValue(document.NewValueBuffer(types.NewIntegerValue(10))), "[10]"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := test.value.MarshalJSON()
			assert.NoError(t, err)
			require.Equal(t, test.expected, string(data))
		})
	}
}
