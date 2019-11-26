package genji_test

//go:generate genji gen -s User -f example_test.go

import (
	"fmt"
	"os"

	"github.com/asdine/genji"
	"github.com/asdine/genji/record"
)

type User struct {
	ID   int64
	Name string
	Age  uint32
}

func Example() {
	// Create a database instance, here we'll store everything in memory
	db, err := genji.Open(":memory:")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Create a table. Genji tables are schemaless, you don't need to specify a schema.
	err = db.Exec("CREATE TABLE IF NOT EXISTS user")
	if err != nil {
		panic(err)
	}

	// Create an index.
	err = db.Exec("CREATE INDEX IF NOT EXISTS idx_user_name ON user (name)")
	if err != nil {
		panic(err)
	}

	// Insert some data
	err = db.Exec("INSERT INTO user (id, name, age) VALUES (?, ?, ?)", 10, "foo", 15)
	if err != nil {
		panic(err)
	}

	// Since the user structure implements the record.Record interface, we can use it with the
	// RECORDS clause.
	err = db.Exec("INSERT INTO user RECORDS ?, ?", &User{ID: 1, Name: "bar", Age: 100}, &User{ID: 2, Name: "baz"})
	if err != nil {
		panic(err)
	}

	// Query some records
	stream, err := db.Query("SELECT id, name, age FROM user WHERE id > ?", 1)
	if err != nil {
		panic(err)
	}
	// always close the result when you're done with it
	defer stream.Close()

	// Iterate over the results
	err = stream.Iterate(func(r record.Record) error {
		var id int
		var name string
		var age int32

		err = record.Scan(r, &id, &name, &age)
		if err != nil {
			return err
		}

		fmt.Println(id, name, age)
		return nil
	})

	// Count results
	count, err := stream.Count()
	if err != nil {
		panic(err)
	}
	fmt.Println("Count:", count)

	// Get first record from the results
	r, err := stream.First()
	if err != nil {
		panic(err)
	}
	var id int
	var name string
	var age int32
	err = record.Scan(r, &id, &name, &age)
	if err != nil {
		panic(err)
	}

	// Apply some transformations
	err = stream.
		// Filter all even ids
		Filter(func(r record.Record) (bool, error) {
			f, err := r.GetField("id")
			if err != nil {
				return false, err
			}
			id, err := f.DecodeToInt()
			return id%2 == 0, nil
		}).
		// Enrich the records with a new field
		Map(func(r record.Record) (record.Record, error) {
			var fb record.FieldBuffer

			err := fb.ScanRecord(r)
			if err != nil {
				return nil, err
			}

			fb.Add(record.NewStringField("group", "admin"))
			return &fb, nil
		}).
		// Iterate on them
		Iterate(func(r record.Record) error {
			return record.Dump(os.Stdout, r)
		})

	if err != nil {
		panic(err)
	}

	// Output:
	// 10 foo 15
	// 2 baz 0
	// Count: 2
	// id(Int): 10
	// name(String): "foo"
	// age(Int): 15
	// group(String): "admin"
	// id(Int64): 2
	// name(String): "baz"
	// age(Uint32): 0x0
	// group(String): "admin"
}
