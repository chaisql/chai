package record

import (
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

	require.Equal(t, "age", f.Header.FieldHeaders[0].Name)
	require.EqualValues(t, 3, f.Header.FieldHeaders[0].NameSize)
	require.EqualValues(t, 1, f.Header.FieldHeaders[0].Size)
	require.EqualValues(t, field.Int64, f.Header.FieldHeaders[0].Type)

	require.Equal(t, "name", f.Header.FieldHeaders[1].Name)
	require.EqualValues(t, 4, f.Header.FieldHeaders[1].NameSize)
	require.EqualValues(t, 4, f.Header.FieldHeaders[1].Size)
	require.EqualValues(t, field.String, f.Header.FieldHeaders[1].Type)
}

func BenchmarkDecodeFormat(b *testing.B) {
	data, err := Encode(FieldBuffer([]field.Field{
		field.NewInt64("age", 10),
		field.NewString("name", "john"),
		field.NewString("address", "some address"),
	}))
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeFormat(data)
	}
}
