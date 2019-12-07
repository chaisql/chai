package document_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/asdine/genji/document"
	"github.com/stretchr/testify/require"
)

func TestFormat(t *testing.T) {
	data, err := document.Encode(document.NewFieldBuffer().
		Add("age", document.NewInt64Value(10)).
		Add("address", document.NewNullValue()).
		Add("name", document.NewStringValue("john")))

	require.NoError(t, err)

	var f document.Format
	err = f.Decode(data)
	require.NoError(t, err)
	require.Equal(t, len(f.Body), f.Header.BodySize())
	require.EqualValues(t, 3, f.Header.FieldsCount)
	require.Len(t, f.Header.FieldHeaders, 3)

	require.EqualValues(t, "address", f.Header.FieldHeaders[0].Name)
	require.EqualValues(t, 7, f.Header.FieldHeaders[0].NameSize)
	require.EqualValues(t, 0, f.Header.FieldHeaders[0].Size)
	require.EqualValues(t, document.NullValue, f.Header.FieldHeaders[0].Type)
	require.EqualValues(t, 0, f.Header.FieldHeaders[0].Offset)

	require.EqualValues(t, "age", f.Header.FieldHeaders[1].Name)
	require.EqualValues(t, 3, f.Header.FieldHeaders[1].NameSize)
	require.EqualValues(t, 8, f.Header.FieldHeaders[1].Size)
	require.EqualValues(t, document.Int64Value, f.Header.FieldHeaders[1].Type)
	require.EqualValues(t, 0, f.Header.FieldHeaders[1].Offset)

	require.EqualValues(t, "name", f.Header.FieldHeaders[2].Name)
	require.EqualValues(t, 4, f.Header.FieldHeaders[2].NameSize)
	require.EqualValues(t, 4, f.Header.FieldHeaders[2].Size)
	require.EqualValues(t, document.StringValue, f.Header.FieldHeaders[2].Type)
	require.EqualValues(t, 8, f.Header.FieldHeaders[2].Offset)

	// ensure using a pointer to FieldBuffer has the same behaviour
	fb := document.NewFieldBuffer().
		Add("age", document.NewInt64Value(10)).
		Add("address", document.NewNullValue()).
		Add("name", document.NewStringValue("john"))

	dataPtr, err := document.Encode(fb)
	require.NoError(t, err)
	require.Equal(t, data, dataPtr)
}

func TestDecodeValue(t *testing.T) {
	doc := document.NewFieldBuffer().
		Add("age", document.NewInt64Value(10)).
		Add("address", document.NewNullValue()).
		Add("name", document.NewStringValue("john"))

	data, err := document.Encode(doc)
	require.NoError(t, err)

	v, err := document.DecodeValue(data, "age")
	require.NoError(t, err)
	require.Equal(t, document.NewInt64Value(10), v)

	v, err = document.DecodeValue(data, "address")
	require.NoError(t, err)
	require.Equal(t, document.NewNullValue(), v)

	v, err = document.DecodeValue(data, "name")
	require.NoError(t, err)
	require.Equal(t, document.NewStringValue("john"), v)
}

func TestEncodeDecode(t *testing.T) {
	tests := []struct {
		name     string
		d        document.Document
		expected string
	}{
		{
			"document.FieldBuffer",
			document.NewFieldBuffer().
				Add("age", document.NewInt64Value(10)).
				Add("name", document.NewStringValue("john")),
			`{"age": 10, "name": "john"}`,
		},
		{
			"Map",
			document.NewFromMap(map[string]interface{}{
				"age":  10,
				"name": "john",
			}),
			`{"age": 10, "name": "john"}`,
		},
		{
			"Nested Document",
			document.NewFieldBuffer().
				Add("age", document.NewInt64Value(10)).
				Add("name", document.NewStringValue("john")).
				Add("address", document.NewDocumentValue(document.NewFromMap(map[string]interface{}{
					"city":    "Ajaccio",
					"country": "France",
				}))),
			`{"age": 10, "name": "john", "address": {"city": "Ajaccio", "country": "France"}}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			enc, err := document.Encode(test.d)
			require.NoError(t, err)
			var buf bytes.Buffer
			err = document.ToJSON(&buf, document.EncodedDocument(enc))
			require.NoError(t, err)
			require.JSONEq(t, test.expected, buf.String())
		})
	}
}

func TestEncodedDocument(t *testing.T) {
	doc := document.NewFieldBuffer().
		Add("age", document.NewInt64Value(10)).
		Add("name", document.NewStringValue("john")).
		Add("address", document.NewDocumentValue(document.NewFromMap(map[string]interface{}{
			"city":    "Ajaccio",
			"country": "France",
		})))

	data, err := document.Encode(doc)
	require.NoError(t, err)

	ec := document.EncodedDocument(data)
	v, err := ec.GetByField("age")
	require.NoError(t, err)
	require.Equal(t, document.NewInt64Value(10), v)
	v, err = ec.GetByField("address")
	require.NoError(t, err)
	var expected, actual bytes.Buffer
	err = document.ToJSON(&expected, document.NewFieldBuffer().Add("address", document.NewDocumentValue(document.NewFromMap(map[string]interface{}{
		"city":    "Ajaccio",
		"country": "France",
	}))))
	require.NoError(t, err)
	err = document.ToJSON(&actual, document.NewFieldBuffer().Add("address", v))
	require.NoError(t, err)
	require.JSONEq(t, expected.String(), actual.String())

	var i int
	err = ec.Iterate(func(f string, v document.Value) error {
		switch f {
		case "age":
			require.Equal(t, document.NewInt64Value(10), v)
		case "address":
			var expected, actual bytes.Buffer
			err = document.ToJSON(&expected, document.NewFieldBuffer().Add("address", document.NewDocumentValue(document.NewFromMap(map[string]interface{}{
				"city":    "Ajaccio",
				"country": "France",
			}))))
			require.NoError(t, err)
			err = document.ToJSON(&actual, document.NewFieldBuffer().Add(f, v))
			require.NoError(t, err)
			require.JSONEq(t, expected.String(), actual.String())
		case "name":
			require.Equal(t, document.NewStringValue("john"), v)
		}
		i++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, i)
}

func TestEncodeArray(t *testing.T) {
	tests := []struct {
		name     string
		a        document.Array
		expected string
	}{
		{
			"Complex array",
			document.NewValueBuffer().
				Append(document.NewInt64Value(10)).
				Append(document.NewStringValue("john")).
				Append(document.NewDocumentValue(document.NewFromMap(map[string]interface{}{
					"city":    "Ajaccio",
					"country": "France",
				}))).
				Append(document.NewArrayValue(document.NewValueBuffer().Append(document.NewInt64Value(11)))),
			`[10, "john", {"city": "Ajaccio", "country": "France"}, [11]]`,
		},
		{
			"Empty array",
			document.NewValueBuffer(),
			`[]`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := document.EncodeArray(test.a)
			require.NoError(t, err)
			var buf bytes.Buffer
			document.ArrayToJSON(&buf, document.EncodedArray(data))
			require.JSONEq(t, test.expected, buf.String())
		})
	}
}

func BenchmarkEncode(b *testing.B) {
	var buf document.FieldBuffer

	for i := int64(0); i < 100; i++ {
		buf.Add(fmt.Sprintf("name-%d", i), document.NewInt64Value(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		document.Encode(&buf)
	}
}

func BenchmarkFormatDecode(b *testing.B) {
	var buf document.FieldBuffer

	for i := int64(0); i < 100; i++ {
		buf.Add(fmt.Sprintf("name-%d", i), document.NewInt64Value(i))
	}

	data, err := document.Encode(&buf)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var f document.Format
		f.Decode(data)
	}
}

func BenchmarkDecodeValue(b *testing.B) {
	var buf document.FieldBuffer

	for i := int64(0); i < 100; i++ {
		buf.Add(fmt.Sprintf("name-%d", i), document.NewInt64Value(i))
	}

	data, err := document.Encode(&buf)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		document.DecodeValue(data, "name-99")
	}
}

func BenchmarkEncodedDocument(b *testing.B) {
	var buf document.FieldBuffer

	for i := int64(0); i < 100; i++ {
		buf.Add(fmt.Sprintf("name-%d", i), document.NewInt64Value(i))
	}

	data, err := document.Encode(&buf)
	require.NoError(b, err)

	ec := document.EncodedDocument(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ec.Iterate(func(string, document.Value) error {
			return nil
		})
	}
}
