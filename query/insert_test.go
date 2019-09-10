package query

import (
	"testing"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	"github.com/stretchr/testify/require"
)

func TestInsertStatement(t *testing.T) {
	t.Run("NoFields", func(t *testing.T) {
		tx, cleanup := createTable(t, 10, false)
		defer cleanup()

		res := Insert().Into(Table("test")).Values(IntValue(5), StringValue("hello"), IntValue(50), IntValue(5)).Exec(tx)
		require.Error(t, res.Err())
	})

	t.Run("WithFields", func(t *testing.T) {
		tx, cleanup := createTable(t, 10, false)
		defer cleanup()

		res := Insert().Into(Table("test")).Fields("a", "b").Values(IntValue(5), StringValue("hello")).Exec(tx)
		require.NoError(t, res.Err())

		tb, err := tx.GetTable("test")
		require.NoError(t, err)

		st := table.NewStream(tb)
		count, err := st.Count()
		require.NoError(t, err)
		require.Equal(t, 11, count)

		_, rec, err := res.First()
		require.NoError(t, err)

		rIDf, err := rec.GetField("recordID")
		require.NoError(t, err)

		rec, err = tb.GetRecord(rIDf.Data)
		require.NoError(t, err)
		expected := record.FieldBuffer([]field.Field{
			field.NewInt("a", 5),
			field.NewString("b", "hello"),
		})
		d, err := record.Encode(expected)
		require.NoError(t, err)

		require.EqualValues(t, d, []byte(rec.(record.EncodedRecord)))
	})
}
