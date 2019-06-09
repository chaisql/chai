package genji_test

import (
	"fmt"
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

func TestTableString(t *testing.T) {
	tests := []struct {
		name       string
		withSchema bool
		expected   string
	}{
		{"No schema", false, `name(String): "John 0", age(Int): 10
name(String): "John 1", age(Int): 11
name(String): "John 2", age(Int): 12
`},
		{"With schema", true, `name(String), age(Int)
"John 0", 10
"John 1", 11
"John 2", 12
`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.New(memory.NewEngine())
			require.NoError(t, err)

			err = db.Update(func(tx *genji.Tx) error {
				if test.withSchema {
					err := tx.CreateTableWithSchema("test", &record.Schema{
						Fields: []field.Field{
							{Name: "name", Type: field.String},
							{Name: "age", Type: field.Int},
						},
					})
					require.NoError(t, err)
				} else {
					err := tx.CreateTable("test")
					require.NoError(t, err)
				}

				tb, err := tx.Table("test")
				require.NoError(t, err)

				for i := 0; i < 3; i++ {
					rowid, err := tb.Insert(record.FieldBuffer([]field.Field{
						field.NewString("name", fmt.Sprintf("John %d", i)),
						field.NewInt("age", 10+i),
					}))
					require.NoError(t, err)
					require.NotNil(t, rowid)
				}

				s := tb.(*genji.Table).String()
				require.Equal(t, test.expected, s)
				return nil
			})
			require.NoError(t, err)

		})
	}
}
