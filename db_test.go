package genji_test

import (
	"testing"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/stretchr/testify/require"
)

func TestTable(t *testing.T) {
	t.Run("Table/Insert/NoIndex", func(t *testing.T) {
		db, err := genji.New(memory.NewEngine())
		require.NoError(t, err)

		err = db.Update(func(tx *genji.Tx) error {
			err := tx.CreateTable("test")
			require.NoError(t, err)

			tb, err := tx.Table("test")
			require.NoError(t, err)

			rowid, err := tb.Insert(record.FieldBuffer([]field.Field{
				field.NewString("name", "John"),
				field.NewInt64("age", 10),
			}))
			require.NoError(t, err)
			require.NotNil(t, rowid)

			m, err := tx.Indexes("test")
			require.NoError(t, err)
			require.Empty(t, m)

			return nil
		})
		require.NoError(t, err)
	})

	t.Run("Table/Insert/WithIndex", func(t *testing.T) {
		db, err := genji.New(memory.NewEngine())
		require.NoError(t, err)
		defer db.Close()

		err = db.Update(func(tx *genji.Tx) error {
			err := tx.CreateTable("test")
			require.NoError(t, err)

			tb, err := tx.Table("test")
			require.NoError(t, err)

			err = tx.CreateIndex("test", "name")
			require.NoError(t, err)

			rowid, err := tb.Insert(record.FieldBuffer([]field.Field{
				field.NewString("name", "John"),
				field.NewInt64("age", 10),
			}))
			require.NoError(t, err)
			require.NotNil(t, rowid)

			m, err := tx.Indexes("test")
			require.NoError(t, err)
			require.NotEmpty(t, m)

			c := m["name"].Cursor()
			v, rid := c.Seek([]byte("John"))
			require.Equal(t, []byte("John"), v)
			require.Equal(t, rowid, rid)

			return nil
		})
		require.NoError(t, err)
	})
}
