package encoding_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/encoding"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func TestOrdering(t *testing.T) {
	tests := []struct {
		input interface{}
		tp    byte
	}{
		// null first
		{nil, encoding.NullValue},

		// then bool
		{false, encoding.FalseValue},
		{true, encoding.TrueValue},

		// then integers
		{int64(math.MinInt64), encoding.Int64Value},
		{int64(math.MinInt32), encoding.Int64Value},
		{int64(math.MinInt16), encoding.Int64Value},
		{int64(math.MinInt8), encoding.Int64Value},
		{int64(-33), encoding.Int64Value},
		{int64(-32), encoding.Int64Value},
		{int64(0), encoding.Int64Value},
		{int64(127), encoding.Int64Value},
		{int64(128), encoding.Int64Value},
		{int64(math.MaxInt8), encoding.Int64Value},
		{int64(math.MaxInt16), encoding.Int64Value},
		{int64(math.MaxInt32), encoding.Int64Value},
		{int64(math.MaxInt64), encoding.Int64Value},

		// then floats
		{float64(math.SmallestNonzeroFloat64), encoding.Float64Value},
		{float64(math.SmallestNonzeroFloat32), encoding.Float64Value},
		{float64(100), encoding.Float64Value},
	}

	var prev []byte
	var previnput interface{}
	for i, test := range tests {
		var x []byte
		switch test.tp {
		case encoding.NullValue:
			x = encoding.EncodeNull(nil)
		case encoding.FalseValue, encoding.TrueValue:
			x = encoding.EncodeBoolean(nil, test.input.(bool))
		case encoding.Int64Value:
			x = encoding.EncodeInt(nil, test.input.(int64))
		case encoding.Float64Value:
			x = encoding.EncodeFloat(nil, test.input.(float64))
		}

		if prev == nil {
			prev = x
			previnput = tests[i].input
			continue
		}

		require.True(t, encoding.Compare(prev, x) <= 0, "input %v: %v < %v", i, previnput, test.input)
	}
}

func TestEncodeDecode(t *testing.T) {
	userMapDoc := document.NewFromMap(map[string]any{
		"age":  10,
		"name": "john",
	})

	addressMapDoc := document.NewFromMap(map[string]any{
		"city":    "Ajaccio",
		"country": "France",
	})

	complexArray := document.NewValueBuffer().
		Append(types.NewBoolValue(true)).
		Append(types.NewIntegerValue(-40)).
		Append(types.NewDoubleValue(-3.14)).
		Append(types.NewDoubleValue(3)).
		Append(types.NewBlobValue([]byte("blob"))).
		Append(types.NewTextValue("hello")).
		Append(types.NewDocumentValue(addressMapDoc)).
		Append(types.NewArrayValue(document.NewValueBuffer().Append(types.NewIntegerValue(11))))

	tests := []struct {
		name     string
		d        types.Document
		expected string
		fails    bool
	}{
		{
			"empty doc",
			document.NewFieldBuffer(),
			`{}`,
			false,
		},
		{
			"document.FieldBuffer",
			document.NewFieldBuffer().
				Add("age", types.NewIntegerValue(10)).
				Add("name", types.NewTextValue("john")),
			`{"age": 10, "name": "john"}`,
			false,
		},
		{
			"Map",
			userMapDoc,
			`{"age": 10, "name": "john"}`,
			false,
		},
		{
			"duplicate field name",
			document.NewFieldBuffer().
				Add("age", types.NewIntegerValue(10)).
				Add("age", types.NewIntegerValue(10)),
			``,
			true,
		},
		{
			"Nested types.Document",
			document.NewFieldBuffer().
				Add("age", types.NewIntegerValue(10)).
				Add("name", types.NewTextValue("john")).
				Add("address", types.NewDocumentValue(addressMapDoc)).
				Add("array", types.NewArrayValue(complexArray)),
			`{"age": 10, "name": "john", "address": {"city": "Ajaccio", "country": "France"}, "array": [true, -40, -3.14, 3, "YmxvYg==", "hello", {"city": "Ajaccio", "country": "France"}, [11]]}`,
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buf, err := encoding.EncodeValue(nil, types.NewDocumentValue(test.d), false)
			if test.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			v, _ := encoding.DecodeValue(buf, false)

			data, err := v.MarshalJSON()
			require.NoError(t, err)
			require.JSONEq(t, test.expected, string(data))
		})
	}
}

func TestEncodeDecodeBooleans(t *testing.T) {
	tests := []struct {
		input bool
		want  []byte
	}{
		{false, []byte{encoding.FalseValue}},
		{true, []byte{encoding.TrueValue}},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.input), func(t *testing.T) {
			got := encoding.EncodeBoolean(nil, test.input)
			require.Equal(t, test.want, got)

			x := encoding.DecodeBoolean(got)
			require.Equal(t, test.input, x)
		})
	}
}

func TestEncodeDecodeNull(t *testing.T) {
	got := encoding.EncodeNull(nil)
	require.Equal(t, []byte{0x02}, got)
}

func mustNewKey(t testing.TB, namespace tree.Namespace, order tree.SortOrder, values ...types.Value) []byte {
	k := tree.NewKey(values...)

	b, err := k.Encode(namespace, order)
	require.NoError(t, err)

	return b
}
