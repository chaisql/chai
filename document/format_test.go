package document_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/asdine/genji/document"
	"github.com/stretchr/testify/require"
)

func TestFormat(t *testing.T) {
	data, err := document.Encode(document.FieldBuffer([]document.Field{
		document.NewInt64Field("age", 10),
		document.NewNullField("address"),
		document.NewStringField("name", "john"),
	}))
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
	require.EqualValues(t, document.Null, f.Header.FieldHeaders[0].Type)
	require.EqualValues(t, 0, f.Header.FieldHeaders[0].Offset)

	require.EqualValues(t, "age", f.Header.FieldHeaders[1].Name)
	require.EqualValues(t, 3, f.Header.FieldHeaders[1].NameSize)
	require.EqualValues(t, 8, f.Header.FieldHeaders[1].Size)
	require.EqualValues(t, document.Int64, f.Header.FieldHeaders[1].Type)
	require.EqualValues(t, 0, f.Header.FieldHeaders[1].Offset)

	require.EqualValues(t, "name", f.Header.FieldHeaders[2].Name)
	require.EqualValues(t, 4, f.Header.FieldHeaders[2].NameSize)
	require.EqualValues(t, 4, f.Header.FieldHeaders[2].Size)
	require.EqualValues(t, document.String, f.Header.FieldHeaders[2].Type)
	require.EqualValues(t, 8, f.Header.FieldHeaders[2].Offset)

	// ensure using a pointer to FieldBuffer has the same behaviour
	fb := document.FieldBuffer([]document.Field{
		document.NewInt64Field("age", 10),
		document.NewNullField("address"),
		document.NewStringField("name", "john"),
	})
	dataPtr, err := document.Encode(&fb)
	require.NoError(t, err)
	require.Equal(t, data, dataPtr)
}

func TestDecodeField(t *testing.T) {
	rec := document.FieldBuffer([]document.Field{
		document.NewInt64Field("age", 10),
		document.NewNullField("address"),
		document.NewStringField("name", "john"),
	})

	data, err := document.Encode(rec)
	require.NoError(t, err)

	f, err := document.DecodeField(data, "address")
	require.NoError(t, err)
	require.Equal(t, rec[0], f)

	f, err = document.DecodeField(data, "age")
	require.NoError(t, err)
	require.Equal(t, rec[1], f)

	f, err = document.DecodeField(data, "name")
	require.NoError(t, err)
	require.Equal(t, rec[2], f)
}

func TestEncodeDecode(t *testing.T) {
	tests := []struct {
		name string
		r    document.Document
	}{
		{
			"document.FieldBuffer",
			document.FieldBuffer([]document.Field{
				document.NewInt64Field("age", 10),
				document.NewStringField("name", "john"),
			}),
		},
		{
			"Map",
			document.NewFromMap(map[string]interface{}{
				"age":  10,
				"name": "john",
			}),
		},
		{
			"Nested Record",
			document.FieldBuffer([]document.Field{
				document.NewInt64Field("age", 10),
				document.NewStringField("name", "john"),
				document.NewObjectField("address", document.NewFromMap(map[string]interface{}{
					"city":    "Ajaccio",
					"country": "France",
				})),
			}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			enc, err := document.Encode(test.r)
			require.NoError(t, err)
			var buf1, buf2 bytes.Buffer
			err = document.ToJSON(&buf1, document.EncodedRecord(enc))
			require.NoError(t, err)
			err = document.ToJSON(&buf2, test.r)
			require.NoError(t, err)
			require.JSONEq(t, buf2.String(), buf1.String())
		})
	}
}

func TestEncodedRecord(t *testing.T) {
	rec := document.FieldBuffer([]document.Field{
		document.NewInt64Field("age", 10),
		document.NewStringField("name", "john"),
		document.NewObjectField("address", document.NewFromMap(map[string]interface{}{
			"city":    "Ajaccio",
			"country": "France",
		})),
	})

	data, err := document.Encode(rec)
	require.NoError(t, err)

	ec := document.EncodedRecord(data)
	f, err := ec.GetValueByName("age")
	require.NoError(t, err)
	require.Equal(t, document.NewInt64Field("age", 10), f)
	f, err = ec.GetValueByName("address")
	require.NoError(t, err)
	var expected, actual bytes.Buffer
	err = document.ToJSON(&expected, document.FieldBuffer{document.NewObjectField("address", document.NewFromMap(map[string]interface{}{
		"city":    "Ajaccio",
		"country": "France",
	}))})
	require.NoError(t, err)
	err = document.ToJSON(&actual, document.FieldBuffer{f})
	require.NoError(t, err)
	require.JSONEq(t, expected.String(), actual.String())

	var i int
	err = ec.Iterate(func(f document.Field) error {
		switch f.Name {
		case "age":
			require.Equal(t, document.NewInt64Field("age", 10), f)
		case "address":
			var expected, actual bytes.Buffer
			err = document.ToJSON(&expected, document.FieldBuffer{document.NewObjectField("address", document.NewFromMap(map[string]interface{}{
				"city":    "Ajaccio",
				"country": "France",
			}))})
			require.NoError(t, err)
			err = document.ToJSON(&actual, document.FieldBuffer{f})
			require.NoError(t, err)
			require.JSONEq(t, expected.String(), actual.String())
		case "name":
			require.Equal(t, document.NewStringField("name", "john"), f)
		}
		i++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, i)
}

func BenchmarkEncode(b *testing.B) {
	var fields []document.Field

	for i := int64(0); i < 100; i++ {
		fields = append(fields, document.NewInt64Field(fmt.Sprintf("name-%d", i), i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		document.Encode(document.FieldBuffer(fields))
	}
}

func BenchmarkFormatDecode(b *testing.B) {
	var fields []document.Field

	for i := int64(0); i < 100; i++ {
		fields = append(fields, document.NewInt64Field(fmt.Sprintf("name-%d", i), i))
	}

	data, err := document.Encode(document.FieldBuffer(fields))
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var f document.Format
		f.Decode(data)
	}
}

func BenchmarkDecodeField(b *testing.B) {
	var fields []document.Field

	for i := int64(0); i < 100; i++ {
		fields = append(fields, document.NewInt64Field(fmt.Sprintf("name-%d", i), i))
	}
	data, err := document.Encode(document.FieldBuffer(fields))
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		document.DecodeField(data, "name-99")
	}
}

func BenchmarkEncodedRecord(b *testing.B) {
	var fields []document.Field

	for i := int64(0); i < 100; i++ {
		fields = append(fields, document.NewInt64Field(fmt.Sprintf("name-%d", i), i))
	}
	data, err := document.Encode(document.FieldBuffer(fields))
	require.NoError(b, err)

	ec := document.EncodedRecord(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ec.Iterate(func(document.Field) error {
			return nil
		})
	}
}
