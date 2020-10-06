package genji_test

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
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
	err = db.Exec("CREATE TABLE user")
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
	err = stream.Iterate(func(d document.Document) error {
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

	// Count results
	count, err := stream.Count()
	if err != nil {
		panic(err)
	}
	fmt.Println("Count:", count)

	// Get first document from the results
	d, err := stream.First()
	if err != nil {
		panic(err)
	}

	// Scan into a struct
	var u User
	err = document.StructScan(d, &u)
	if err != nil {
		panic(err)
	}

	enc := json.NewEncoder(os.Stdout)

	// Apply some manual transformations
	err = stream.
		// Filter all even ids
		Filter(func(d document.Document) (bool, error) {
			v, err := d.GetByField("id")
			if err != nil {
				return false, err
			}
			return v.V.(int64)%2 == 0, err
		}).
		// Enrich the documents with a new field
		Map(func(d document.Document) (document.Document, error) {
			var fb document.FieldBuffer

			err := fb.ScanDocument(d)
			if err != nil {
				return nil, err
			}

			fb.Add("group", document.NewTextValue("admin"))
			return &fb, nil
		}).
		// Iterate on them
		Iterate(func(d document.Document) error {
			return enc.Encode(d)
		})

	if err != nil {
		panic(err)
	}

	// Output:
	// {10 foo 15 { }}
	// {12 bar 16 {Lyon 69001}}
	// {2 bat 0 { }}
	// Count: 3
	// {"id":10,"name":"foo","age":15,"group":"admin"}
	// {"id":12,"name":"bar","age":16,"address":{"city":"Lyon","zipcode":"69001"},"group":"admin"}
	// {"id":2,"name":"bat","age":0,"address":{"city":"","zipcode":""},"group":"admin"}
}
