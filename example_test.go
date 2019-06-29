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
	db, err := genji.New(ng)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// generated User store
	users := NewUserStore(db)

	// generated type safe methods
	err = users.Insert(&User{
		ID:   10,
		Name: "foo",
		Age:  32,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Get a user
	u, err := users.Get(10)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(u)

	// List users
	list, err := users.List(0, 10)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(list)

	// complex queries
	qs := NewUserQuerySelector()
	var result UserResult
	err = users.View(func(tx *genji.Tx) error {
		// SELECT ID, Name FROM User where Age >= 18
		return query.Select(qs.ID, qs.Name).From(qs.Table()).Where(qs.Age.Gte(18)).
			Run(tx).
			Scan(&result)
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result)
}

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
