package driver_test

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/genjidb/genji/driver"
)

type User struct {
	ID   int64
	Name string
	Age  uint32
}

func Example() {
	db, err := sql.Open("genji", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS user")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("CREATE INDEX IF NOT EXISTS idx_user_name ON user (name)")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("INSERT INTO user (id, name, age) VALUES (?, ?, ?)", 10, "foo", 15)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("INSERT INTO user VALUES ?, ?", &User{ID: 1, Name: "bar", Age: 100}, &User{ID: 2, Name: "baz"})
	if err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query("SELECT * FROM user WHERE name = ?", "bar")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var u User
		err = rows.Scan(driver.Scanner(&u))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(u)
	}

	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

	// Output: {1 bar 100}
}
