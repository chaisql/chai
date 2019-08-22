package record_test

import (
	"testing"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/stretchr/testify/require"
)

var _ record.Record = new(record.FieldBuffer)

func TestFieldBuffer(t *testing.T) {
	b := record.FieldBuffer([]field.Field{
		field.NewInt64("a", 10),
		field.NewString("b", "hello"),
	})

	var i int
	err := b.Iterate(func(f field.Field) error {
		require.NotEmpty(t, f)
		require.Equal(t, f, b[i])
		i++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, i)
}

func TestNewFromMap(t *testing.T) {
	m := map[string]interface{}{
		"Name": "foo",
		"Age":  10,
	}

	rec := record.NewFromMap(m)

	t.Run("Iterate", func(t *testing.T) {
		counter := make(map[string]int)

		err := rec.Iterate(func(f field.Field) error {
			counter[f.Name]++
			v, err := field.Decode(f)
			require.NoError(t, err)
			require.Equal(t, m[f.Name], v)
			return nil
		})
		require.NoError(t, err)
		require.Len(t, counter, 2)
		require.Equal(t, counter["Name"], 1)
		require.Equal(t, counter["Age"], 1)
	})

	t.Run("Field", func(t *testing.T) {
		f, err := rec.GetField("Name")
		require.NoError(t, err)
		require.Equal(t, field.Field{Name: "Name", Type: field.String, Data: []byte("foo")}, f)

		f, err = rec.GetField("Age")
		require.NoError(t, err)
		require.Equal(t, field.Field{Name: "Age", Type: field.Int, Data: field.EncodeInt(10)}, f)

		_, err = rec.GetField("bar")
		require.Error(t, err)
	})
}
