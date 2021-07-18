// Package encodingtest provides a test suite for testing codec implementations.
package encodingtest

import (
	"bytes"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

// TestCodec runs a list of tests on the given codec.
func TestCodec(t *testing.T, codecBuilder func() encoding.Codec) {
	tests := []struct {
		name string
		test func(*testing.T, func() encoding.Codec)
	}{
		{"EncodeDecode", testEncodeDecode},
		{"NewDocument", testDecodeDocument},
		{"Document/GetByField", testDocumentGetByField},
		{"Array/GetByIndex", testArrayGetByIndex},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.test(t, codecBuilder)
		})
	}
}

func testEncodeDecode(t *testing.T, codecBuilder func() encoding.Codec) {
	userMapDoc, err := document.NewFromMap(map[string]interface{}{
		"age":  10,
		"name": "john",
	})
	require.NoError(t, err)

	addressMapDoc, err := document.NewFromMap(map[string]string{
		"city":    "Ajaccio",
		"country": "France",
	})
	require.NoError(t, err)

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
	}{
		{
			"document.FieldBuffer",
			document.NewFieldBuffer().
				Add("age", types.NewIntegerValue(10)).
				Add("name", types.NewTextValue("john")),
			`{"age": 10, "name": "john"}`,
		},
		{
			"Map",
			userMapDoc,
			`{"age": 10, "name": "john"}`,
		},
		{
			"Nested types.Document",
			document.NewFieldBuffer().
				Add("age", types.NewIntegerValue(10)).
				Add("name", types.NewTextValue("john")).
				Add("address", types.NewDocumentValue(addressMapDoc)).
				Add("array", types.NewArrayValue(complexArray)),
			`{"age": 10, "name": "john", "address": {"city": "Ajaccio", "country": "France"}, "array": [true, -40, -3.14, 3, "YmxvYg==", "hello", {"city": "Ajaccio", "country": "France"}, [11]]}`,
		},
	}

	var buf bytes.Buffer
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buf.Reset()
			codec := codecBuilder()
			err := codec.NewEncoder(&buf).EncodeDocument(test.d)
			require.NoError(t, err)
			ok, err := types.IsEqual(types.NewDocumentValue(test.d), types.NewDocumentValue(codec.NewDecoder(buf.Bytes())))
			require.NoError(t, err)
			require.True(t, ok)
			data, err := document.MarshalJSON(codec.NewDecoder(buf.Bytes()))
			require.NoError(t, err)
			require.JSONEq(t, test.expected, string(data))
		})
	}
}

func testDocumentGetByField(t *testing.T, codecBuilder func() encoding.Codec) {
	codec := codecBuilder()

	fb := document.NewFieldBuffer().
		Add("a", types.NewIntegerValue(10)).
		Add("b", types.NewNullValue()).
		Add("c", types.NewTextValue("john"))

	var buf bytes.Buffer

	err := codec.NewEncoder(&buf).EncodeDocument(fb)
	require.NoError(t, err)

	d := codec.NewDecoder(buf.Bytes())

	v, err := d.GetByField("a")
	require.NoError(t, err)

	require.Equal(t, types.NewIntegerValue(10), v)

	v, err = d.GetByField("b")
	require.NoError(t, err)
	require.Equal(t, types.NewNullValue(), v)

	v, err = d.GetByField("c")
	require.NoError(t, err)
	require.Equal(t, types.NewTextValue("john"), v)

	v, err = d.GetByField("d")
	require.Equal(t, document.ErrFieldNotFound, err)
}

func testArrayGetByIndex(t *testing.T, codecBuilder func() encoding.Codec) {
	codec := codecBuilder()

	arr := document.NewValueBuffer().
		Append(types.NewIntegerValue(10)).
		Append(types.NewNullValue()).
		Append(types.NewTextValue("john"))

	var buf bytes.Buffer

	err := codec.NewEncoder(&buf).EncodeDocument(document.NewFieldBuffer().Add("a", types.NewArrayValue(arr)))
	require.NoError(t, err)

	d := codec.NewDecoder(buf.Bytes())
	v, err := d.GetByField("a")
	require.NoError(t, err)

	require.Equal(t, types.ArrayValue, v.Type())
	a := v.V().(types.Array)
	v, err = a.GetByIndex(0)
	require.NoError(t, err)

	require.Equal(t, types.NewIntegerValue(10), v)

	v, err = a.GetByIndex(1)
	require.NoError(t, err)
	require.Equal(t, types.NewNullValue(), v)

	v, err = a.GetByIndex(2)
	require.NoError(t, err)
	require.Equal(t, types.NewTextValue("john"), v)

	v, err = a.GetByIndex(1000)
	require.Equal(t, document.ErrValueNotFound, err)
}

func testDecodeDocument(t *testing.T, codecBuilder func() encoding.Codec) {
	codec := codecBuilder()

	mapDoc, err := document.NewFromMap(map[string]string{
		"city":    "Ajaccio",
		"country": "France",
	})
	require.NoError(t, err)

	doc := document.NewFieldBuffer().
		Add("age", types.NewIntegerValue(10)).
		Add("name", types.NewTextValue("john")).
		Add("address", types.NewDocumentValue(mapDoc))

	var buf bytes.Buffer

	enc := codec.NewEncoder(&buf)
	defer enc.Close()

	err = enc.EncodeDocument(doc)
	require.NoError(t, err)

	ec := codec.NewDecoder(buf.Bytes())
	v, err := ec.GetByField("age")
	require.NoError(t, err)
	require.Equal(t, types.NewIntegerValue(10), v)
	v, err = ec.GetByField("address")
	require.NoError(t, err)
	expected, err := document.MarshalJSON(document.NewFieldBuffer().Add("address", types.NewDocumentValue(mapDoc)))
	require.NoError(t, err)
	actual, err := document.MarshalJSON(document.NewFieldBuffer().Add("address", v))
	require.NoError(t, err)
	require.JSONEq(t, string(expected), string(actual))

	var i int
	err = ec.Iterate(func(f string, v types.Value) error {
		switch f {
		case "age":
			require.Equal(t, types.NewIntegerValue(10), v)
		case "address":
			expected, err := document.MarshalJSON(document.NewFieldBuffer().Add("address", types.NewDocumentValue(mapDoc)))
			require.NoError(t, err)
			actual, err := document.MarshalJSON(document.NewFieldBuffer().Add(f, v))
			require.NoError(t, err)
			require.JSONEq(t, string(expected), string(actual))
		case "name":
			require.Equal(t, types.NewTextValue("john"), v)
		}
		i++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, i)
}
