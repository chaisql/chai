package types_test

import (
	"math"
	"testing"
	"time"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/testutil"
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
		{"int", int32(10), "10"},
		{"float64", 10.0, "10.0"},
		{"float64", 10.1, "10.1"},
		{"float64", math.MaxFloat64, "1.7976931348623157e+308"},
		{"time", now, `"` + now.UTC().Format(time.RFC3339Nano) + `"`},
		{"null", nil, "NULL"},
		{"time", now, `"` + now.UTC().Format(time.RFC3339Nano) + `"`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v, err := row.NewValue(test.value)
			require.NoError(t, err)
			data, err := v.MarshalText()
			require.NoError(t, err)
			require.Equal(t, test.expected, string(data))
			if test.name != "time" {
				e := testutil.ParseExpr(t, string(data))
				got, err := e.Eval(&environment.Environment{})
				require.NoError(t, err)
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
		{"bytea", types.NewByteaValue([]byte("bar")), `"YmFy"`},
		{"string", types.NewTextValue("bar"), `"bar"`},
		{"bool", types.NewBooleanValue(true), "true"},
		{"int", types.NewIntegerValue(10), "10"},
		{"double", types.NewDoubleValue(10.1), "10.1"},
		{"time", types.NewTimestampValue(now), `"` + now.UTC().Format(time.RFC3339Nano) + `"`},
		{"double with no decimal", types.NewDoubleValue(10), "10"},
		{"big double", types.NewDoubleValue(1e15), "1e+15"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := test.value.MarshalJSON()
			require.NoError(t, err)
			require.Equal(t, test.expected, string(data))
		})
	}
}
