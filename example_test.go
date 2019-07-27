package genji_test

//go:generate genji -s User -f example_test.go

import (
	"fmt"
	"log"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/query"
	"github.com/asdine/genji/record"
)

type User struct {
	ID   int64  `genji:"pk"`
	Name string `genji:"index"`
	Age  uint32
}

func Example() {
	ng := memory.NewEngine()
	db := genji.New(ng)
	defer db.Close()

	// open a read-write transaction
	err := db.Update(func(tx *genji.Tx) error {
		// create a table
		err := tx.CreateTable("foo")
		if err != nil {
			return err
		}

		// get the table
		t, err := tx.Table("foo")
		if err != nil {
			return err
		}

		// insert a User, no reflection involved
		recordID, err := t.Insert(&User{
			ID:   10,
			Name: "foo",
			Age:  32,
		})
		if err != nil {
			return err
		}
		fmt.Println(recordID)

		// Use generated types to run queries
		qs := NewUserQuerySelector()
		var result UserResult

		// SELECT ID, Name FROM foo where Age >= 18
		return query.Select(qs.ID, qs.Name).From(t).Where(qs.Age.Gte(18)).
			Run(tx).
			Scan(&result)
	})
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleDB() {
	ng := memory.NewEngine()
	db := genji.New(ng)
	defer db.Close()

	err := db.Update(func(tx *genji.Tx) error {
		err := tx.CreateTable("Table")
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
