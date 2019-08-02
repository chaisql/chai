package genji_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	"github.com/asdine/genji/table/tabletest"
	"github.com/stretchr/testify/require"
)

func tableBuilder(t testing.TB) func() (table.Table, func()) {
	return func() (table.Table, func()) {
		db := genji.New(memory.NewEngine())
		tx, err := db.Begin(true)
		require.NoError(t, err)

		err = tx.CreateTable("test")
		require.NoError(t, err)

		tb, err := tx.Table("test")
		require.NoError(t, err)

		return tb, func() {
			tx.Rollback()
		}
	}
}

func TestTable(t *testing.T) {
	tabletest.TestSuite(t, tableBuilder(t))
	// 	t.Run("Table/Insert/NoIndex", func(t *testing.T) {
	// 		db := genji.New(memory.NewEngine())

	// 		err := db.Update(func(tx *genji.Tx) error {
	// 			err := tx.CreateTable("test")
	// 			require.NoError(t, err)

	// 			tb, err := tx.Table("test")
	// 			require.NoError(t, err)

	// 			recordID, err := tb.Insert(record.FieldBuffer([]field.Field{
	// 				field.NewString("name", "John"),
	// 				field.NewInt64("age", 10),
	// 			}))
	// 			require.NoError(t, err)
	// 			require.NotNil(t, recordID)

	// 			m, err := tx.Indexes("test")
	// 			require.NoError(t, err)
	// 			require.Empty(t, m)

	// 			return nil
	// 		})
	// 		require.NoError(t, err)
	// 	})

	// 	t.Run("Table/Insert/WithIndex", func(t *testing.T) {
	// 		db := genji.New(memory.NewEngine())
	// 		defer db.Close()

	// 		err := db.Update(func(tx *genji.Tx) error {
	// 			err := tx.CreateTable("test")
	// 			require.NoError(t, err)

	// 			tb, err := tx.Table("test")
	// 			require.NoError(t, err)

	// 			err = tx.CreateIndex("test", "name")
	// 			require.NoError(t, err)

	// 			recordID, err := tb.Insert(record.FieldBuffer([]field.Field{
	// 				field.NewString("name", "John"),
	// 				field.NewInt64("age", 10),
	// 			}))
	// 			require.NoError(t, err)
	// 			require.NotNil(t, recordID)

	// 			m, err := tx.Indexes("test")
	// 			require.NoError(t, err)
	// 			require.NotEmpty(t, m)

	// 			c := m["name"].Cursor()
	// 			v, rid := c.Seek([]byte("John"))
	// 			require.Equal(t, []byte("John"), v)
	// 			require.Equal(t, recordID, rid)

	// 			return nil
	// 		})
	// 		require.NoError(t, err)
	// 	})
}

func TestTableString(t *testing.T) {
	tests := []struct {
		name     string
		expected string
	}{
		{"OK", `name(String): "John 0", age(Int): 10
name(String): "John 1", age(Int): 11
name(String): "John 2", age(Int): 12
`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := genji.New(memory.NewEngine())

			err := db.Update(func(tx *genji.Tx) error {
				err := tx.CreateTable("test")
				require.NoError(t, err)

				tb, err := tx.Table("test")
				require.NoError(t, err)

				for i := 0; i < 3; i++ {
					recordID, err := tb.Insert(record.FieldBuffer([]field.Field{
						field.NewString("name", fmt.Sprintf("John %d", i)),
						field.NewInt("age", 10+i),
					}))
					require.NoError(t, err)
					require.NotNil(t, recordID)
				}

				var buf bytes.Buffer
				err = table.Dump(&buf, tb)
				require.NoError(t, err)
				require.Equal(t, test.expected, buf.String())
				return nil
			})
			require.NoError(t, err)

		})
	}
}
