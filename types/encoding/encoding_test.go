package encoding_test

import (
	"bytes"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
	"github.com/genjidb/genji/types/encoding"
	"github.com/stretchr/testify/require"
)

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

	var buf bytes.Buffer
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buf.Reset()
			err := encoding.EncodeValue(&buf, types.NewDocumentValue(test.d))
			if test.fails {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			v, err := encoding.DecodeValue(buf.Bytes())
			assert.NoError(t, err)

			ok, err := types.IsEqual(types.NewDocumentValue(test.d), v)
			assert.NoError(t, err)
			require.True(t, ok)

			data, err := v.MarshalJSON()
			assert.NoError(t, err)
			require.JSONEq(t, test.expected, string(data))
		})
	}
}

func TestDocumentGetByField(t *testing.T) {
	fb := document.NewFieldBuffer().
		Add("a", types.NewIntegerValue(10)).
		Add("b", types.NewNullValue()).
		Add("c", types.NewTextValue("john"))

	var buf bytes.Buffer

	err := encoding.EncodeValue(&buf, types.NewDocumentValue(fb))
	assert.NoError(t, err)

	v, err := encoding.DecodeValue(buf.Bytes())
	assert.NoError(t, err)

	d := v.V().(types.Document)

	v, err = d.GetByField("a")
	assert.NoError(t, err)

	ok, err := types.IsEqual(types.NewIntegerValue(10), v)
	require.NoError(t, err)
	require.True(t, ok)

	v, err = d.GetByField("b")
	assert.NoError(t, err)

	ok, err = types.IsEqual(types.NewNullValue(), v)
	require.NoError(t, err)
	require.True(t, ok)

	v, err = d.GetByField("c")
	assert.NoError(t, err)

	ok, err = types.IsEqual(types.NewTextValue("john"), v)
	require.NoError(t, err)
	require.True(t, ok)

	_, err = d.GetByField("d")
	assert.ErrorIs(t, err, types.ErrFieldNotFound)
}

func TestArrayGetByIndex(t *testing.T) {
	arr := document.NewValueBuffer().
		Append(types.NewIntegerValue(10)).
		Append(types.NewNullValue()).
		Append(types.NewTextValue("john")).
		Append(types.NewArrayValue(document.NewValueBuffer().
			Append(types.NewIntegerValue(11)).
			Append(types.NewNullValue()),
		))

	var buf bytes.Buffer

	err := encoding.EncodeValue(&buf, types.NewDocumentValue(document.NewFieldBuffer().Add("a", types.NewArrayValue(arr))))
	assert.NoError(t, err)

	v, err := encoding.DecodeValue(buf.Bytes())
	assert.NoError(t, err)

	d := v.V().(types.Document)

	v, err = d.GetByField("a")
	assert.NoError(t, err)

	require.Equal(t, types.ArrayValue, v.Type())
	a := v.V().(types.Array)
	v, err = a.GetByIndex(0)
	assert.NoError(t, err)

	ok, err := types.IsEqual(types.NewIntegerValue(10), v)
	assert.NoError(t, err)
	require.True(t, ok)

	v, err = a.GetByIndex(1)
	assert.NoError(t, err)

	ok, err = types.IsEqual(types.NewNullValue(), v)
	assert.NoError(t, err)
	require.True(t, ok)

	v, err = a.GetByIndex(2)
	assert.NoError(t, err)

	ok, err = types.IsEqual(types.NewTextValue("john"), v)
	assert.NoError(t, err)
	require.True(t, ok)

	v, err = a.GetByIndex(3)
	assert.NoError(t, err)

	ok, err = types.IsEqual(types.NewArrayValue(document.NewValueBuffer().
		Append(types.NewIntegerValue(11)).
		Append(types.NewNullValue()),
	), v)
	assert.NoError(t, err)
	require.True(t, ok)

	_, err = a.GetByIndex(1000)
	assert.ErrorIs(t, err, types.ErrValueNotFound)
}

func TestDecodeDocument(t *testing.T) {
	mapDoc := document.NewFromMap(map[string]string{
		"city":    "Ajaccio",
		"country": "France",
	})

	doc := document.NewFieldBuffer().
		Add("age", types.NewIntegerValue(10)).
		Add("name", types.NewTextValue("john")).
		Add("address", types.NewDocumentValue(mapDoc))

	var buf bytes.Buffer

	err := encoding.EncodeValue(&buf, types.NewDocumentValue(doc))
	assert.NoError(t, err)

	ec, err := encoding.DecodeValue(buf.Bytes())
	assert.NoError(t, err)

	d := ec.V().(types.Document)

	v, err := d.GetByField("age")
	assert.NoError(t, err)

	ok, err := types.IsEqual(types.NewIntegerValue(10), v)
	require.NoError(t, err)
	require.True(t, ok)
	v, err = d.GetByField("address")
	assert.NoError(t, err)
	expected, err := document.MarshalJSON(document.NewFieldBuffer().Add("address", types.NewDocumentValue(mapDoc)))
	assert.NoError(t, err)
	actual, err := document.MarshalJSON(document.NewFieldBuffer().Add("address", v))
	assert.NoError(t, err)
	require.JSONEq(t, string(expected), string(actual))

	var i int
	err = d.Iterate(func(f string, v types.Value) error {
		switch f {
		case "age":
			ok, err := types.IsEqual(types.NewIntegerValue(10), v)
			require.NoError(t, err)
			require.True(t, ok)
		case "address":
			expected, err := document.MarshalJSON(document.NewFieldBuffer().Add("address", types.NewDocumentValue(mapDoc)))
			assert.NoError(t, err)
			actual, err := document.MarshalJSON(document.NewFieldBuffer().Add(f, v))
			assert.NoError(t, err)
			require.JSONEq(t, string(expected), string(actual))
		case "name":
			ok, err := types.IsEqual(types.NewTextValue("john"), v)
			require.NoError(t, err)
			require.True(t, ok)
		}
		i++
		return nil
	})
	assert.NoError(t, err)
	require.Equal(t, 3, i)

	t.Run("empty document", func(t *testing.T) {
		var buf bytes.Buffer
		err := encoding.EncodeValue(&buf, types.NewDocumentValue(document.NewFieldBuffer()))
		assert.NoError(t, err)

		ec, err := encoding.DecodeValue(buf.Bytes())
		assert.NoError(t, err)

		d := ec.V().(types.Document)

		i := 0
		err = d.Iterate(func(field string, value types.Value) error {
			i++
			return nil
		})
		assert.NoError(t, err)
		require.Equal(t, 0, i)
	})
}

func TestArrayIterate(t *testing.T) {
	addressMapDoc := document.NewFromMap(map[string]string{
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

	var buf bytes.Buffer

	err := encoding.EncodeValue(&buf, types.NewArrayValue(complexArray))
	assert.NoError(t, err)

	ec, err := encoding.DecodeValue(buf.Bytes())
	assert.NoError(t, err)

	a := ec.V().(types.Array)

	var i int
	err = a.Iterate(func(idx int, v types.Value) error {
		ok, err := types.IsEqual(complexArray.Values[idx], v)
		require.NoError(t, err)
		require.True(t, ok)

		i++
		return nil
	})
	assert.NoError(t, err)
	require.Equal(t, 8, i)

	t.Run("empty array", func(t *testing.T) {
		var buf bytes.Buffer

		err = encoding.EncodeValue(&buf, types.NewArrayValue(document.NewValueBuffer()))
		assert.NoError(t, err)

		ec, err := encoding.DecodeValue(buf.Bytes())
		assert.NoError(t, err)

		a := ec.V().(types.Array)

		var i int
		err = a.Iterate(func(idx int, v types.Value) error {
			i++
			return nil
		})
		assert.NoError(t, err)
		require.Equal(t, 0, i)
	})
}
