package genji_test

import (
	"fmt"
	"log"
	"testing"

	"github.com/asdine/genji"
	"github.com/asdine/genji/database"
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/engine/memoryengine"
	"github.com/stretchr/testify/require"
)

func ExampleDB_SQLDB() {
	db, err := genji.New(memoryengine.NewEngine())
	if err != nil {
		log.Fatal(err)
	}
	dbx := db.SQLDB()
	defer db.Close()

	_, err = dbx.Exec("CREATE TABLE IF NOT EXISTS user")
	if err != nil {
		log.Fatal(err)
	}

	_, err = dbx.Exec("CREATE INDEX IF NOT EXISTS idx_user_name ON user (name)")
	if err != nil {
		log.Fatal(err)
	}

	_, err = dbx.Exec("INSERT INTO user (id, name, age) VALUES (?, ?, ?)", 10, "foo", 15)
	if err != nil {
		log.Fatal(err)
	}

	_, err = dbx.Exec("INSERT INTO user VALUES ?, ?", &User{ID: 1, Name: "bar", Age: 100}, &User{ID: 2, Name: "baz"})
	if err != nil {
		log.Fatal(err)
	}

	rows, err := dbx.Query("SELECT * FROM user WHERE name = ?", "bar")
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

func ExampleTx() {
	db, err := genji.New(memoryengine.NewEngine())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin(false)
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()

	err = tx.Exec("CREATE TABLE IF NOT EXISTS user")
	if err != nil {
		log.Fatal(err)
	}

	err = tx.Exec("INSERT INTO user (id, name, age) VALUES (?, ?, ?)", 10, "foo", 15)
	if err != nil {
		log.Fatal(err)
	}

	result, err := tx.Query("SELECT id, name, age FROM user WHERE name = ?", "foo")
	if err != nil {
		panic(err)
	}
	defer result.Close()

	var u User
	r, err := result.First()
	if err != nil {
		panic(err)
	}

	err = u.ScanDocument(r)
	if err != nil {
		panic(err)
	}

	fmt.Println(u)

	var id uint64
	var name string
	var age uint8

	err = document.Scan(r, &id, &name, &age)
	if err != nil {
		panic(err)
	}

	fmt.Println(id, name, age)

	err = tx.Commit()
	if err != nil {
		panic(err)
	}

	// Output: {10 foo 15}
	// 10 foo 15
}

func TestQueryRecord(t *testing.T) {
	db, err := genji.New(memoryengine.NewEngine())
	require.NoError(t, err)

	tx, err := db.Begin(true)
	require.NoError(t, err)

	err = tx.Exec(`
			CREATE TABLE test;
			INSERT INTO test (a, b) VALUES (1, 'foo'), (2, 'bar')
		`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	t.Run("Should return the first record", func(t *testing.T) {
		var a int
		var b string

		r, err := db.QueryRecord("SELECT * FROM test")
		err = document.Scan(r, &a, &b)
		require.NoError(t, err)
		require.Equal(t, 1, a)
		require.Equal(t, "foo", b)

		tx, err := db.Begin(false)
		require.NoError(t, err)
		defer tx.Rollback()

		r, err = tx.QueryRecord("SELECT * FROM test")
		require.NoError(t, err)
		err = document.Scan(r, &a, &b)
		require.NoError(t, err)
		require.Equal(t, 1, a)
		require.Equal(t, "foo", b)
	})

	t.Run("Should return an error if no record", func(t *testing.T) {
		r, err := db.QueryRecord("SELECT * FROM test WHERE a > 100")
		require.Equal(t, database.ErrRecordNotFound, err)
		require.Nil(t, r)

		tx, err := db.Begin(false)
		require.NoError(t, err)
		defer tx.Rollback()
		r, err = tx.QueryRecord("SELECT * FROM test WHERE a > 100")
		require.Equal(t, database.ErrRecordNotFound, err)
		require.Nil(t, r)
	})
}

func ExampleResult_First() {
	db, err := genji.New(memoryengine.NewEngine())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Exec("CREATE TABLE IF NOT EXISTS user")
	if err != nil {
		log.Fatal(err)
	}

	err = db.Exec("INSERT INTO user (id, name, age) VALUES (?, ?, ?)", 10, "foo", 15)
	if err != nil {
		log.Fatal(err)
	}

	result, err := db.Query("SELECT id, name, age FROM user WHERE name = ?", "foo")
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
	err = u.ScanDocument(r)
	if err != nil {
		panic(err)
	}

	fmt.Println(u)

	// Scan individual variables
	var id uint64
	var name string
	var age uint8

	err = document.Scan(r, &id, &name, &age)
	if err != nil {
		panic(err)
	}

	fmt.Println(id, name, age)

	// Output: {10 foo 15}
	// 10 foo 15
}

func ExampleResult_Iterate() {
	db, err := genji.New(memoryengine.NewEngine())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Exec("CREATE TABLE IF NOT EXISTS user")
	if err != nil {
		log.Fatal(err)
	}

	for i := 1; i <= 10; i++ {
		err = db.Exec("INSERT INTO user VALUES ?", &User{
			ID:   int64(i),
			Name: fmt.Sprintf("foo%d", i),
			Age:  uint32(i * 10),
		})
		if err != nil {
			log.Fatal(err)
		}
	}

	result, err := db.Query(`SELECT id, name, age FROM user WHERE age >= 18`)
	if err != nil {
		panic(err)
	}
	defer result.Close()

	err = result.Iterate(func(r document.Document) error {
		// Scan using generated methods
		var u User
		err = u.ScanDocument(r)
		if err != nil {
			return err
		}

		fmt.Println(u)

		// Or scan individual variables
		var id uint64
		var name string
		var age uint8

		err = document.Scan(r, &id, &name, &age)
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
