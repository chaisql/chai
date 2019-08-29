package genji_test

import (
	"log"
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
