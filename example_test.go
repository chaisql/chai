package chai_test

import (
	"database/sql"
	"fmt"

	_ "github.com/chaisql/chai"
)

type User struct {
	ID   int64
	Name string
	Age  uint32
}

func Example() {
	// Create a database instance, here we'll store everything in memory
	db, err := sql.Open("chai", ":memory:")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Create a table.
	_, err = db.Exec("CREATE TABLE user (id int, name text, age int)")
	if err != nil {
		panic(err)
	}

	// Create an index.
	_, err = db.Exec("CREATE INDEX idx_user_name ON user (name)")
	if err != nil {
		panic(err)
	}

	// Insert some data
	_, err = db.Exec("INSERT INTO user (id, name, age) VALUES (?, ?, ?)", 10, "foo", 15)
	if err != nil {
		panic(err)
	}

	// Query some rows
	rows, err := db.Query("SELECT * FROM user WHERE id > ?", 1)
	if err != nil {
		panic(err)
	}
	// always close the result when you're done with it
	defer rows.Close()

	for rows.Next() {
		var u User

		err := rows.Scan(&u.ID, &u.Name, &u.Age)
		if err != nil {
			panic(err)
		}

		fmt.Println(u)
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}

	// Output:
	// {10 foo 15}
}
