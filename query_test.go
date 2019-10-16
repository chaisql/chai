package genji_test

import (
	"fmt"
	"log"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/record/recordutil"
)

func ExampleResult_First() {
	db, err := genji.New(memory.NewEngine())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Exec("CREATE TABLE user IF NOT EXISTS")
	if err != nil {
		log.Fatal(err)
	}

	err = db.Exec("INSERT INTO user (ID, Name, Age) VALUES (?, ?, ?)", 10, "foo", 15)
	if err != nil {
		log.Fatal(err)
	}

	result, err := db.Query("SELECT * FROM user WHERE Name = ?", "foo")
	if err != nil {
		panic(err)
	}
	defer result.Close()

	r, err := result.First()
	if err != nil {
		panic(err)
	}

	// Scan using generated methods
	var u User
	err = u.ScanRecord(r)
	if err != nil {
		panic(err)
	}

	fmt.Println(u)

	// Scan individual variables
	var id uint64
	var name string
	var age uint8

	err = recordutil.Scan(r, &id, &name, &age)
	if err != nil {
		panic(err)
	}

	fmt.Println(id, name, age)

	// Output: {10 foo 15}
	// 10 foo 15
}

func ExampleResult_Iterate() {
	db, err := genji.New(memory.NewEngine())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Exec("CREATE TABLE user IF NOT EXISTS")
	if err != nil {
		log.Fatal(err)
	}

	for i := 1; i <= 10; i++ {
		err = db.Exec("INSERT INTO user RECORDS ?", &User{
			ID:   int64(i),
			Name: fmt.Sprintf("foo%d", i),
			Age:  uint32(i * 10),
		})
		if err != nil {
			log.Fatal(err)
		}
	}

	result, err := db.Query("SELECT * FROM user WHERE Age >= 18")
	if err != nil {
		panic(err)
	}
	defer result.Close()

	err = result.Iterate(func(r record.Record) error {
		// Scan using generated methods
		var u User
		err = u.ScanRecord(r)
		if err != nil {
			return err
		}

		fmt.Println(u)

		// Or scan individual variables
		var id uint64
		var name string
		var age uint8

		err = recordutil.Scan(r, &id, &name, &age)
		if err != nil {
			return err
		}

		fmt.Println(id, name, age)
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Output: {2 foo2 20}
	// 2 foo2 20
	// {3 foo3 30}
	// 3 foo3 30
	// {4 foo4 40}
	// 4 foo4 40
	// {5 foo5 50}
	// 5 foo5 50
	// {6 foo6 60}
	// 6 foo6 60
	// {7 foo7 70}
	// 7 foo7 70
	// {8 foo8 80}
	// 8 foo8 80
	// {9 foo9 90}
	// 9 foo9 90
	// {10 foo10 100}
	// 10 foo10 100
}
