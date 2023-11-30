package object_test

import (
	"testing"
	"time"

	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/object"
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

	mapAny := map[string]any{
		"a": 1,
		"b": true,
	}

	mapInt := map[string]int{
		"a": 1,
		"b": 2,
	}

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
		{"object", object.NewFieldBuffer().Add("a", types.NewIntegerValue(10)), object.NewFieldBuffer().Add("a", types.NewIntegerValue(10))},
		{"array", object.NewValueBuffer(types.NewIntegerValue(10)), object.NewValueBuffer(types.NewIntegerValue(10))},
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
		{"map[string]any", mapAny, object.NewFromMap(mapAny)},
		{"map[string]int", mapInt, object.NewFromMap(mapInt)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v, err := object.NewValue(test.value)
			assert.NoError(t, err)
			require.Equal(t, test.expected, v.V())
		})
	}
}

func TestNewFromJSON(t *testing.T) {
	tests := []struct {
		name     string
		data     string
		expected *object.FieldBuffer
		fails    bool
	}{
		{"empty object", "{}", object.NewFieldBuffer(), false},
		{"empty object, missing closing bracket", "{", nil, true},
		{"classic object", `{"a": 1, "b": true, "c": "hello", "d": [1, 2, 3], "e": {"f": "g"}}`,
			object.NewFieldBuffer().
				Add("a", types.NewIntegerValue(1)).
				Add("b", types.NewBoolValue(true)).
				Add("c", types.NewTextValue("hello")).
				Add("d", types.NewArrayValue(object.NewValueBuffer().
					Append(types.NewIntegerValue(1)).
					Append(types.NewIntegerValue(2)).
					Append(types.NewIntegerValue(3)))).
				Add("e", types.NewObjectValue(object.NewFieldBuffer().Add("f", types.NewTextValue("g")))),
			false},
		{"string values", `{"a": "hello ciao"}`, object.NewFieldBuffer().Add("a", types.NewTextValue("hello ciao")), false},
		{"+integer values", `{"a": 1000}`, object.NewFieldBuffer().Add("a", types.NewIntegerValue(1000)), false},
		{"-integer values", `{"a": -1000}`, object.NewFieldBuffer().Add("a", types.NewIntegerValue(-1000)), false},
		{"+float values", `{"a": 10000000000.0}`, object.NewFieldBuffer().Add("a", types.NewDoubleValue(10000000000)), false},
		{"-float values", `{"a": -10000000000.0}`, object.NewFieldBuffer().Add("a", types.NewDoubleValue(-10000000000)), false},
		{"bool values", `{"a": true, "b": false}`, object.NewFieldBuffer().Add("a", types.NewBoolValue(true)).Add("b", types.NewBoolValue(false)), false},
		{"empty arrays", `{"a": []}`, object.NewFieldBuffer().Add("a", types.NewArrayValue(object.NewValueBuffer())), false},
		{"nested arrays", `{"a": [[1,  2]]}`, object.NewFieldBuffer().
			Add("a", types.NewArrayValue(
				object.NewValueBuffer().
					Append(types.NewArrayValue(
						object.NewValueBuffer().
							Append(types.NewIntegerValue(1)).
							Append(types.NewIntegerValue(2)))))), false},
		{"missing comma", `{"a": 1 "b": 2}`, nil, true},
		{"missing closing brackets", `{"a": 1, "b": 2`, nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := object.NewFromJSON([]byte(test.data))

			fb := object.NewFieldBuffer()
			err := fb.Copy(d)

			if test.fails {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				require.Equal(t, *test.expected, *fb)
			}
		})
	}

	t.Run("GetByField", func(t *testing.T) {
		d := object.NewFromJSON([]byte(`{"a": 1000}`))

		v, err := d.GetByField("a")
		assert.NoError(t, err)
		require.Equal(t, types.NewIntegerValue(1000), v)

		_, err = d.GetByField("b")
		assert.ErrorIs(t, err, types.ErrFieldNotFound)
	})
}

func TestNewFromMap(t *testing.T) {
	m := map[string]interface{}{
		"name":     "foo",
		"age":      10,
		"nilField": nil,
	}

	doc := object.NewFromMap(m)

	t.Run("Iterate", func(t *testing.T) {
		counter := make(map[string]int)

		err := doc.Iterate(func(f string, v types.Value) error {
			counter[f]++
			switch f {
			case "name":
				require.Equal(t, m[f], types.As[string](v))
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

	t.Run("GetByField", func(t *testing.T) {
		v, err := doc.GetByField("name")
		assert.NoError(t, err)
		require.Equal(t, types.NewTextValue("foo"), v)

		v, err = doc.GetByField("age")
		assert.NoError(t, err)
		require.Equal(t, types.NewIntegerValue(10), v)

		v, err = doc.GetByField("nilField")
		assert.NoError(t, err)
		require.Equal(t, types.NewNullValue(), v)

		_, err = doc.GetByField("bar")
		require.Equal(t, types.ErrFieldNotFound, err)
	})
}

func BenchmarkJSONToObject(b *testing.B) {
	data := []byte(`{"_id":"5f8aefb8e443c6c13afdb305","index":0,"guid":"42c2719e-3371-4b2f-b855-d302a8b7eab0","isActive":true,"balance":"$1,064.79","picture":"http://placehold.it/32x32","age":40,"eyeColor":"blue","name":"Adele Webb","gender":"female","company":"EXTRAGEN","email":"adelewebb@extragen.com","phone":"+1 (964) 409-2397","address":"970 Charles Place, Watrous, Texas, 2522","about":"Amet non do ullamco duis velit sunt esse et cillum nisi mollit ea magna. Tempor ut occaecat proident laborum velit nisi et excepteur exercitation non est labore. Laboris pariatur enim proident et. Qui minim enim et incididunt incididunt adipisicing tempor. Occaecat adipisicing sint ex ut exercitation exercitation voluptate. Laboris adipisicing ut cillum eu cillum est sunt amet Lorem quis pariatur.\r\n","registered":"2016-05-25T10:36:44 -04:00","latitude":64.57112,"longitude":176.136138,"tags":["velit","minim","eiusmod","est","eu","voluptate","deserunt"],"friends":[{"id":0,"name":"Mathis Robertson"},{"id":1,"name":"Cecilia Donaldson"},{"id":2,"name":"Joann Goodwin"}],"greeting":"Hello, Adele Webb! You have 2 unread messages.","favoriteFruit":"apple"}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d := object.NewFromJSON(data)
		d.Iterate(func(string, types.Value) error {
			return nil
		})
	}
}

func TestNewFromCSV(t *testing.T) {
	headers := []string{"a", "b", "c"}
	columns := []string{"A", "B", "C"}

	d := object.NewFromCSV(headers, columns)
	testutil.RequireJSONEq(t, d, `{"a": "A", "b": "B", "c": "C"}`)
}
