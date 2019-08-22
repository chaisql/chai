package genji_test

//go:generate genji -s User -f example_test.go

import (
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

	// open a read-write transaction
	err = db.Update(func(tx *genji.Tx) error {
		t, err := tx.CreateTableIfNotExists("users")
		if err != nil {
			return err
		}

		err = t.CreateIndexesIfNotExist(NewUserIndexes())
		if err != nil {
			return err
		}

		// insert a User, no reflection involved
		_, err = t.Insert(&User{
			ID:   10,
			Name: "foo",
			Age:  32,
		})
		if err != nil {
			return err
		}

		f := NewUserFields()

		var users []User
		// SELECT ID, Name FROM foo where Age >= 18
		return query.Select().From(t).Where(f.Age.Gte(18)).
			Run(tx).
			Iterate(func(recordID []byte, r record.Record) error {
				var u User
				err := u.ScanRecord(r)
				if err != nil {
					return err
				}

				users = append(users, u)
				return nil
			})
	})
	if err != nil {
		log.Fatal(err)
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
