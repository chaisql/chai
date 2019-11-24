package genji_test

import (
	"fmt"
	"log"
	"testing"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/memoryengine"
	"github.com/asdine/genji/record"
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

	_, err = dbx.Exec("INSERT INTO user RECORDS ?, ?", &User{ID: 1, Name: "bar", Age: 100}, &User{ID: 2, Name: "baz"})
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

	err = u.ScanRecord(r)
	if err != nil {
		panic(err)
	}

	fmt.Println(u)

	var id uint64
	var name string
	var age uint8

	err = record.Scan(r, &id, &name, &age)
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
		err = record.Scan(r, &a, &b)
		require.NoError(t, err)
		require.Equal(t, 1, a)
		require.Equal(t, "foo", b)

		tx, err := db.Begin(false)
		require.NoError(t, err)
		defer tx.Rollback()

		r, err = tx.QueryRecord("SELECT * FROM test")
		require.NoError(t, err)
		err = record.Scan(r, &a, &b)
		require.NoError(t, err)
		require.Equal(t, 1, a)
		require.Equal(t, "foo", b)
	})

	t.Run("Should return an error if no record", func(t *testing.T) {
		r, err := db.QueryRecord("SELECT * FROM test WHERE a > 100")
		require.Equal(t, genji.ErrRecordNotFound, err)
		require.Nil(t, r)

		tx, err := db.Begin(false)
		require.NoError(t, err)
		defer tx.Rollback()
		r, err = tx.QueryRecord("SELECT * FROM test WHERE a > 100")
		require.Equal(t, genji.ErrRecordNotFound, err)
		require.Nil(t, r)
	})
}
