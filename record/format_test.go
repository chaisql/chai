package record_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/asdine/genji/record"
	"github.com/asdine/genji/value"
	"github.com/stretchr/testify/require"
)

func TestFormat(t *testing.T) {
	data, err := record.Encode(record.FieldBuffer([]record.Field{
		record.NewInt64Field("age", 10),
		record.NewNullField("address"),
		record.NewStringField("name", "john"),
	}))
	require.NoError(t, err)

	var f record.Format
	err = f.Decode(data)
	require.NoError(t, err)
	require.Equal(t, len(f.Body), f.Header.BodySize())
	require.EqualValues(t, 3, f.Header.FieldsCount)
	require.Len(t, f.Header.FieldHeaders, 3)

	require.EqualValues(t, "age", f.Header.FieldHeaders[0].Name)
	require.EqualValues(t, 3, f.Header.FieldHeaders[0].NameSize)
	require.EqualValues(t, 8, f.Header.FieldHeaders[0].Size)
	require.EqualValues(t, value.Int64, f.Header.FieldHeaders[0].Type)
	require.EqualValues(t, 0, f.Header.FieldHeaders[0].Offset)

	require.EqualValues(t, "address", f.Header.FieldHeaders[1].Name)
	require.EqualValues(t, 7, f.Header.FieldHeaders[1].NameSize)
	require.EqualValues(t, 0, f.Header.FieldHeaders[1].Size)
	require.EqualValues(t, value.Null, f.Header.FieldHeaders[1].Type)
	require.EqualValues(t, 8, f.Header.FieldHeaders[1].Offset)

	require.EqualValues(t, "name", f.Header.FieldHeaders[2].Name)
	require.EqualValues(t, 4, f.Header.FieldHeaders[2].NameSize)
	require.EqualValues(t, 4, f.Header.FieldHeaders[2].Size)
	require.EqualValues(t, value.String, f.Header.FieldHeaders[2].Type)
	require.EqualValues(t, 8, f.Header.FieldHeaders[2].Offset)
}

func TestDecodeField(t *testing.T) {
	rec := record.FieldBuffer([]record.Field{
		record.NewInt64Field("age", 10),
		record.NewNullField("address"),
		record.NewStringField("name", "john"),
	})

	data, err := record.Encode(rec)
	require.NoError(t, err)

	f, err := record.DecodeField(data, "age")
	require.NoError(t, err)
	require.Equal(t, rec[0], f)

	f, err = record.DecodeField(data, "address")
	require.NoError(t, err)
	require.Equal(t, rec[1], f)

	f, err = record.DecodeField(data, "name")
	require.NoError(t, err)
	require.Equal(t, rec[2], f)
}

func TestEncodeDecode(t *testing.T) {
	tests := []struct {
		name string
		r    record.Record
	}{
		{
			"record.FieldBuffer",
			record.FieldBuffer([]record.Field{
				record.NewInt64Field("age", 10),
				record.NewStringField("name", "john"),
			}),
		},
		{
			"Map",
			record.NewFromMap(map[string]interface{}{
				"age":  10,
				"name": "john",
			}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			enc, err := record.Encode(test.r)
			require.NoError(t, err)
			var buf1, buf2 bytes.Buffer
			err = record.ToJSON(&buf1, record.EncodedRecord(enc))
			require.NoError(t, err)
			err = record.ToJSON(&buf2, test.r)
			require.NoError(t, err)
			require.JSONEq(t, buf2.String(), buf1.String())
		})
	}
}

func TestEncodedRecord(t *testing.T) {
	rec := record.FieldBuffer([]record.Field{
		record.NewInt64Field("age", 10),
		record.NewStringField("name", "john"),
	})

	data, err := record.Encode(rec)
	require.NoError(t, err)

	ec := record.EncodedRecord(data)
	f, err := ec.GetField("age")
	require.NoError(t, err)
	require.Equal(t, rec[0], f)

	var i int
	err = ec.Iterate(func(f record.Field) error {
		require.Equal(t, rec[i], f)
		i++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, i)
}

func BenchmarkEncode(b *testing.B) {
	var fields []record.Field

	for i := int64(0); i < 100; i++ {
		fields = append(fields, record.NewInt64Field(fmt.Sprintf("name-%d", i), i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record.Encode(record.FieldBuffer(fields))
	}
}

func BenchmarkFormatDecode(b *testing.B) {
	var fields []record.Field

	for i := int64(0); i < 100; i++ {
		fields = append(fields, record.NewInt64Field(fmt.Sprintf("name-%d", i), i))
	}

	data, err := record.Encode(record.FieldBuffer(fields))
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var f record.Format
		f.Decode(data)
	}
}

func BenchmarkDecodeField(b *testing.B) {
	var fields []record.Field

	for i := int64(0); i < 100; i++ {
		fields = append(fields, record.NewInt64Field(fmt.Sprintf("name-%d", i), i))
	}
	data, err := record.Encode(record.FieldBuffer(fields))
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record.DecodeField(data, "name-99")
	}
}

func BenchmarkEncodedRecord(b *testing.B) {
	var fields []record.Field

	for i := int64(0); i < 100; i++ {
		fields = append(fields, record.NewInt64Field(fmt.Sprintf("name-%d", i), i))
	}
	data, err := record.Encode(record.FieldBuffer(fields))
	require.NoError(b, err)

	ec := record.EncodedRecord(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ec.Iterate(func(record.Field) error {
			return nil
		})
	}
}
