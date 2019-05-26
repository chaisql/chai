package genji_test

import (
	"log"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
)

func ExampleDB() {
	ng := memory.NewEngine()
	db, err := genji.New(ng)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *genji.Tx) error {
		err = tx.CreateTable("Table")
		if err != nil {
			return err
		}

		t, err := tx.Table("Table")
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
