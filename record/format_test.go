package record

import (
	"fmt"
	"testing"

	"github.com/asdine/genji/field"
	"github.com/stretchr/testify/require"
)

func TestFormat(t *testing.T) {
	data, err := Encode(FieldBuffer([]field.Field{
		field.NewInt64("age", 10),
		field.NewString("name", "john"),
	}))
	require.NoError(t, err)

	f, err := DecodeFormat(data)
	require.NoError(t, err)
	require.Equal(t, len(f.Body), f.Header.BodySize())
	require.Len(t, f.Header.FieldHeaders, 2)

	require.EqualValues(t, "age", f.Header.FieldHeaders[0].Name)
	require.EqualValues(t, 3, f.Header.FieldHeaders[0].NameSize)
	require.EqualValues(t, 1, f.Header.FieldHeaders[0].Size)
	require.EqualValues(t, field.Int64, f.Header.FieldHeaders[0].Type)
	require.EqualValues(t, 0, f.Header.FieldHeaders[0].Offset)

	require.EqualValues(t, "name", f.Header.FieldHeaders[1].Name)
	require.EqualValues(t, 4, f.Header.FieldHeaders[1].NameSize)
	require.EqualValues(t, 4, f.Header.FieldHeaders[1].Size)
	require.EqualValues(t, field.String, f.Header.FieldHeaders[1].Type)
	require.EqualValues(t, 1, f.Header.FieldHeaders[1].Offset)
}

func TestDecodeField(t *testing.T) {
	rec := FieldBuffer([]field.Field{
		field.NewInt64("age", 10),
		field.NewString("name", "john"),
	})

	data, err := Encode(rec)
	require.NoError(t, err)

	f, err := DecodeField(data, "age")
	require.NoError(t, err)
	require.Equal(t, rec[0], *f)

	f, err = DecodeField(data, "name")
	require.NoError(t, err)
	require.Equal(t, rec[1], *f)
}

func BenchmarkDecodeField(b *testing.B) {
	var fields []field.Field

	for i := int64(0); i < 100; i++ {
		fields = append(fields, field.NewInt64(fmt.Sprintf("name-%d", i), i))
	}
	data, err := Encode(FieldBuffer(fields))
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeField(data, "name-99")
	}
}

func BenchmarkEncode(b *testing.B) {
	var fields []field.Field

	for i := int64(0); i < 100; i++ {
		fields = append(fields, field.NewInt64(fmt.Sprintf("name-%d", i), i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Encode(FieldBuffer(fields))
	}
}
