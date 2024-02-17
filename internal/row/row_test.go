package row_test

import (
	"testing"
	"time"

	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/testutil/assert"
	"github.com/chaisql/chai/internal/types"
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
		{"time", now, now.UTC()},
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
			v, err := row.NewValue(test.value)
			assert.NoError(t, err)
			require.Equal(t, test.expected, v.V())
		})
	}
}

func TestNewFromMap(t *testing.T) {
	m := map[string]interface{}{
		"name":     "foo",
		"age":      10,
		"nilField": nil,
	}

	r := row.NewFromMap(m)

	t.Run("Iterate", func(t *testing.T) {
		counter := make(map[string]int)

		err := r.Iterate(func(f string, v types.Value) error {
			counter[f]++
			switch f {
			case "name":
				require.Equal(t, m[f], types.AsString(v))
			default:
				require.EqualValues(t, m[f], v.V())
			}
			return nil
		})
		assert.NoError(t, err)
		require.Len(t, counter, 3)
		require.Equal(t, counter["name"], 1)
		require.Equal(t, counter["age"], 1)
		require.Equal(t, counter["nilField"], 1)
	})

	t.Run("Get", func(t *testing.T) {
		v, err := r.Get("name")
		assert.NoError(t, err)
		require.Equal(t, types.NewTextValue("foo"), v)

		v, err = r.Get("age")
		assert.NoError(t, err)
		require.Equal(t, types.NewBigintValue(10), v)

		v, err = r.Get("nilField")
		assert.NoError(t, err)
		require.Equal(t, types.NewNullValue(), v)

		_, err = r.Get("bar")
		require.ErrorIs(t, err, types.ErrColumnNotFound)
	})
}

func TestNewFromCSV(t *testing.T) {
	headers := []string{"a", "b", "c"}
	columns := []string{"A", "B", "C"}

	d := row.NewFromCSV(headers, columns)
	testutil.RequireJSONEq(t, d, `{"a": "A", "b": "B", "c": "C"}`)
}
