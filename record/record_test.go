package record_test

import (
	"testing"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/value"
	"github.com/stretchr/testify/require"
)

var _ record.Record = new(record.FieldBuffer)

func TestFieldBuffer(t *testing.T) {
	buf := record.NewFieldBuffer(
		field.NewInt64("a", 10),
		field.NewString("b", "hello"),
	)

	t.Run("Iterate", func(t *testing.T) {
		var i int
		err := buf.Iterate(func(f field.Field) error {
			require.NotEmpty(t, f)
			require.Equal(t, f, buf[i])
			i++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, i)
	})

	t.Run("Add", func(t *testing.T) {
		buf := record.NewFieldBuffer(
			field.NewInt64("a", 10),
			field.NewString("b", "hello"),
		)

		c := field.NewBool("c", true)
		buf.Add(c)
		require.Len(t, buf, 3)
		require.Equal(t, buf[2], c)
	})

	t.Run("ScanRecord", func(t *testing.T) {
		buf1 := record.NewFieldBuffer(
			field.NewInt64("a", 10),
			field.NewString("b", "hello"),
		)

		buf2 := record.NewFieldBuffer(
			field.NewInt64("a", 20),
			field.NewString("b", "bye"),
			field.NewBool("c", true),
		)

		err := buf1.ScanRecord(buf2)
		require.NoError(t, err)

		require.Equal(t, record.NewFieldBuffer(
			field.NewInt64("a", 10),
			field.NewString("b", "hello"),
			field.NewInt64("a", 20),
			field.NewString("b", "bye"),
			field.NewBool("c", true),
		), buf1)
	})

	t.Run("GetField", func(t *testing.T) {
		f, err := buf.GetField("a")
		require.NoError(t, err)
		require.Equal(t, field.NewInt64("a", 10), f)

		f, err = buf.GetField("not existing")
		require.Error(t, err)
		require.Zero(t, f)
	})

	t.Run("Set", func(t *testing.T) {
		buf1 := record.NewFieldBuffer(
			field.NewInt64("a", 10),
			field.NewString("b", "hello"),
		)

		buf1.Set(field.NewInt64("a", 11))
		require.Equal(t, field.NewInt64("a", 11), buf1[0])

		buf1.Set(field.NewInt64("c", 12))
		require.Len(t, buf1, 3)
		require.Equal(t, field.NewInt64("c", 12), buf1[2])
	})

	t.Run("Delete", func(t *testing.T) {
		buf1 := record.NewFieldBuffer(
			field.NewInt64("a", 10),
			field.NewString("b", "hello"),
		)

		err := buf1.Delete("a")
		require.NoError(t, err)
		require.Len(t, buf1, 1)
		require.Equal(t, record.NewFieldBuffer(
			field.NewString("b", "hello"),
		), buf1)

		err = buf1.Delete("b")
		require.NoError(t, err)
		require.Len(t, buf1, 0)

		err = buf1.Delete("b")
		require.Error(t, err)
	})

	t.Run("Replace", func(t *testing.T) {
		buf1 := record.NewFieldBuffer(
			field.NewInt64("a", 10),
			field.NewString("b", "hello"),
		)

		err := buf1.Replace("a", field.NewInt64("c", 10))
		require.NoError(t, err)
		require.Equal(t, record.NewFieldBuffer(
			field.NewInt64("c", 10),
			field.NewString("b", "hello"),
		), buf1)

		err = buf1.Replace("d", field.NewInt64("c", 11))
		require.Error(t, err)
	})
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
			v, err := f.Decode()
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
		require.Equal(t, field.Field{Name: "Name", Value: value.Value{Type: value.String, Data: []byte("foo")}}, f)

		f, err = rec.GetField("Age")
		require.NoError(t, err)
		require.Equal(t, field.Field{Name: "Age", Value: value.Value{Type: value.Int, Data: value.EncodeInt(10)}}, f)

		_, err = rec.GetField("bar")
		require.Error(t, err)
	})
}
