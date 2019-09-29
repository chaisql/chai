package database_test

import (
	"log"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/record"
)

func ExampleDB() {
	ng := memory.NewEngine()
	db, err := database.New(ng)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *database.Tx) error {
		t, err := tx.CreateTable("Table")
		if err != nil {
			return err
		}

		r := record.FieldBuffer{
			record.NewStringField("Name", "foo"),
			record.NewIntField("Age", 10),
		}

		_, err = t.Insert(r)
		return err
	})
	if err != nil {
		log.Fatal(err)
	}
}
