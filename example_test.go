package genji_test

//go:generate genji -s User -f example_test.go

import (
	"fmt"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/memory"
)

type User struct {
	ID   int64 `genji:"pk"`
	Name string
	Age  uint32
}

func Example() {
	db, err := genji.New(memory.NewEngine())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Exec("CREATE TABLE user IF NOT EXISTS")
	if err != nil {
		panic(err)
	}

	err = db.Exec("CREATE INDEX IF NOT EXISTS idx_user_Name ON user (Name)")
	if err != nil {
		panic(err)
	}

	err = db.Exec("INSERT INTO user (ID, Name, Age) VALUES (?, ?, ?)", 10, "foo", 15)
	if err != nil {
		panic(err)
	}

	err = db.Exec("INSERT INTO user RECORDS ?, ?", &User{ID: 1, Name: "bar", Age: 100}, &User{ID: 2, Name: "baz"})
	if err != nil {
		panic(err)
	}

	stream, err := db.Query("SELECT * FROM user WHERE Name = ?", "bar")
	if err != nil {
		panic(err)
	}
	defer stream.Close()

	var u User
	r, err := stream.First()
	if err != nil {
		panic(err)
	}
	err = u.ScanRecord(r)
	if err != nil {
		panic(err)
	}

	fmt.Println(u)

	// Output: {1 bar 100}
}
