package genji_test

import (
	"fmt"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/types"
)

type User struct {
	ID      int64
	Name    string
	Age     uint32
	Address struct {
		City    string
		ZipCode string
	}
}

func Example() {
	// Create a database instance, here we'll store everything in memory
	db, err := genji.Open(":memory:")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Create a table. Genji tables are schemaless by default, you don't need to specify a schema.
	err = db.Exec("CREATE TABLE user (name text, ...)")
	if err != nil {
		panic(err)
	}

	// Create an index.
	err = db.Exec("CREATE INDEX idx_user_name ON user (name)")
	if err != nil {
		panic(err)
	}

	// Insert some data
	err = db.Exec("INSERT INTO user (id, name, age) VALUES (?, ?, ?)", 10, "foo", 15)
	if err != nil {
		panic(err)
	}

	// Insert some data using document notation
	err = db.Exec(`INSERT INTO user VALUES {id: 12, "name": "bar", age: ?, address: {city: "Lyon", zipcode: "69001"}}`, 16)
	if err != nil {
		panic(err)
	}

	// Structs can be used to describe a document
	err = db.Exec("INSERT INTO user VALUES ?, ?", &User{ID: 1, Name: "baz", Age: 100}, &User{ID: 2, Name: "bat"})
	if err != nil {
		panic(err)
	}

	// Query some documents
	stream, err := db.Query("SELECT * FROM user WHERE id > ?", 1)
	if err != nil {
		panic(err)
	}
	// always close the result when you're done with it
	defer stream.Close()

	// Iterate over the results
	err = stream.Iterate(func(d types.Document) error {
		var u User

		err = document.StructScan(d, &u)
		if err != nil {
			return err
		}

		fmt.Println(u)
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Output:
	// {10 foo 15 { }}
	// {12 bar 16 {Lyon 69001}}
	// {2 bat 0 { }}
}
