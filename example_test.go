package chai_test

import (
	"fmt"

	"github.com/chaisql/chai"
)

type User struct {
	ID   int64
	Name string
	Age  uint32
}

func Example() {
	// Create a database instance, here we'll store everything in memory
	db, err := chai.Open(":memory:")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Create a table.
	err = db.Exec("CREATE TABLE user (id int, name text, age int)")
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

	conn, err := db.Connect()
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// Query some rows
	stream, err := conn.Query("SELECT * FROM user WHERE id > ?", 1)
	if err != nil {
		panic(err)
	}
	// always close the result when you're done with it
	defer stream.Close()

	// Iterate over the results
	err = stream.Iterate(func(r *chai.Row) error {
		var u User

		err = r.StructScan(&u)
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
	// {10 foo 15}
}
