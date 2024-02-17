package driver_test

import (
	"database/sql"
	"fmt"

	"github.com/chaisql/chai/driver"
)

type User struct {
	ID   int64
	Name string
	Age  uint32
}

func Example() {
	db, err := sql.Open("chai", ":memory:")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS user (id INT, name TEXT, age INT)")
	if err != nil {
		panic(err)
	}

	_, err = db.Exec("CREATE INDEX IF NOT EXISTS idx_user_name ON user (name)")
	if err != nil {
		panic(err)
	}

	_, err = db.Exec("INSERT INTO user (id, name, age) VALUES (?, ?, ?)", 10, "foo", 15)
	if err != nil {
		panic(err)
	}

	rows, err := db.Query("SELECT * FROM user WHERE name = ?", "foo")
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var u User
		err = rows.Scan(driver.Scanner(&u))
		if err != nil {
			panic(err)
		}
		fmt.Println(u)
	}

	err = rows.Err()
	if err != nil {
		panic(err)
	}

	// Output: {10 foo 15}
}
