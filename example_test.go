package genji_test

import (
	"fmt"
	"os"

	"github.com/asdine/genji"
	"github.com/asdine/genji/document"
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

	// Structs can be used to describe a document
	err = db.Exec("INSERT INTO user VALUES ?, ?", &User{ID: 1, Name: "bar", Age: 100}, &User{ID: 2, Name: "baz"})
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
		var id int
		var name string
		var age int32

		err = document.Scan(d, &id, &name, &age)
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

	// Get first document from the results
	r, err := stream.First()
	if err != nil {
		panic(err)
	}
	var id int
	var name string
	var age int32
	err = document.Scan(r, &id, &name, &age)
	if err != nil {
		panic(err)
	}

	// Apply some transformations
	err = stream.
		// Filter all even ids
		Filter(func(d document.Document) (bool, error) {
			f, err := r.GetByField("id")
			if err != nil {
				return false, err
			}
			id, err := f.ConvertToInt()
			return id%2 == 0, nil
		}).
		// Enrich the documents with a new field
		Map(func(d document.Document) (document.Document, error) {
			var fb document.FieldBuffer

			err := fb.ScanDocument(r)
			if err != nil {
				return nil, err
			}

			fb.Add("group", document.NewStringValue("admin"))
			return &fb, nil
		}).
		// Iterate on them
		Iterate(func(d document.Document) error {
			return document.ToJSON(os.Stdout, r)
		})

	if err != nil {
		panic(err)
	}

	// Output:
	// 10 foo 15
	// 2 baz 0
	// Count: 2
	// {"id":10,"name":"foo","age":15}
	// {"id":10,"name":"foo","age":15}
}
