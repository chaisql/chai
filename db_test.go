package genji_test

import (
	"bytes"
	"fmt"
	"log"
	"testing"
	"time"

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
		db, err := genji.New(memory.NewEngine())
		require.NoError(t, err)

		tx, err := db.Begin(true)
		require.NoError(t, err)

		tb, err := tx.CreateTable("test")
		require.NoError(t, err)

		return tb, func() {
			tx.Rollback()
		}
	}
}

func TestTable(t *testing.T) {
	tabletest.TestSuite(t, tableBuilder(t))
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
			db, err := genji.New(memory.NewEngine())
			require.NoError(t, err)

			err = db.Update(func(tx *genji.Tx) error {
				tb, err := tx.CreateTable("test")
				require.NoError(t, err)

				for i := 0; i < 3; i++ {
					recordID, err := tb.Insert(record.FieldBuffer([]field.Field{
						field.NewString("name", fmt.Sprintf("John %d", i)),
						field.NewInt("age", 10+i),
					}))
					require.NoError(t, err)
					require.NotNil(t, recordID)
					// sleep 1ms to ensure ordering
					time.Sleep(time.Millisecond)
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

func ExampleDB() {
	ng := memory.NewEngine()
	db, err := genji.New(ng)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *genji.Tx) error {
		t, err := tx.CreateTable("Table")
		if err != nil {
			return err
		}

		r := record.FieldBuffer{
			field.NewString("Name", "foo"),
			field.NewInt("Age", 10),
		}

		_, err = t.Insert(r)
		return err
	})
	if err != nil {
		log.Fatal(err)
	}
}
