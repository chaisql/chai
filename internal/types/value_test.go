package types_test

import (
	"math"
	"testing"
	"time"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/object"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/testutil/assert"
	"github.com/chaisql/chai/internal/types"
	"github.com/stretchr/testify/require"
)

func TestValueMarshalText(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name     string
		value    any
		expected string
	}{
		{"bytes", []byte("bar"), `"\x626172"`},
		{"string", "bar", `"bar"`},
		{"bool", true, "true"},
		{"int", int64(10), "10"},
		{"float64", 10.0, "10.0"},
		{"float64", 10.1, "10.1"},
		{"float64", math.MaxFloat64, "1.7976931348623157e+308"},
		{"time", now, `"` + now.UTC().Format(time.RFC3339Nano) + `"`},
		{"null", nil, "NULL"},
		{"object", object.NewFieldBuffer().
			Add("a", types.NewIntegerValue(10)).
			Add("b c", types.NewTextValue("foo")).
			Add(`"d e"`, types.NewTextValue("foo")),
			"{a: 10, \"b c\": \"foo\", `\"d e\"`: \"foo\"}",
		},
		{"array", object.NewValueBuffer(types.NewIntegerValue(10), types.NewTextValue("foo")), `[10, "foo"]`},
		{"time", now, `"` + now.UTC().Format(time.RFC3339Nano) + `"`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v, err := object.NewValue(test.value)
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
		{"time", now, `"` + now.UTC().Format(time.RFC3339Nano) + `"`},
		{"float64", math.MaxFloat64, "1.7976931348623157e+308"},
		{"null", nil, "NULL"},
		{"object",
			object.NewFieldBuffer().Add("a", types.NewIntegerValue(10)).Add("b c", types.NewTextValue("foo")).Add("d", types.NewArrayValue(object.NewValueBuffer(types.NewIntegerValue(10), types.NewTextValue("foo")))),
			`{
  a: 10,
  "b c": "foo",
  d: [
    10,
    "foo"
  ]
}`},
		{"array",
			object.NewValueBuffer(types.NewIntegerValue(10), types.NewTextValue("foo")),
			`[
  10,
  "foo"
]`,
		},
		{"time", now, `"` + now.UTC().Format(time.RFC3339Nano) + `"`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v, err := object.NewValue(test.value)
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
	now := time.Now()
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
		{"time", types.NewTimestampValue(now), `"` + now.UTC().Format(time.RFC3339Nano) + `"`},
		{"double with no decimal", types.NewDoubleValue(10), "10"},
		{"big double", types.NewDoubleValue(1e15), "1e+15"},
		{"object", types.NewObjectValue(object.NewFieldBuffer().Add("a", types.NewIntegerValue(10))), "{\"a\": 10}"},
		{"array", types.NewArrayValue(object.NewValueBuffer(types.NewIntegerValue(10))), "[10]"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := test.value.MarshalJSON()
			assert.NoError(t, err)
			require.Equal(t, test.expected, string(data))
		})
	}
}
