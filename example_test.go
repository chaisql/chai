package genji_test

//go:generate genji -s User -f example_test.go

import (
	"fmt"
	"log"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/memory"
)

type User struct {
	ID   int64 `genji:"pk"`
	Name string
	Age  uint32
}

func Example() {
	db, err := genji.Open(memory.NewEngine())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE user IF NOT EXISTS")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("CREATE INDEX IF NOT EXISTS idx_user_Name ON user (Name)")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("INSERT INTO user (ID, Name, Age) VALUES (?, ?, ?)", 10, "foo", 15)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("INSERT INTO user RECORDS ?, ?", &User{ID: 1, Name: "bar", Age: 100}, &User{ID: 2, Name: "baz"})
	if err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query("SELECT * FROM user WHERE Name = ?", "bar")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var u User
		err = rows.Scan(&u)
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
