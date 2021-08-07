package genji_test

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func ExampleTx() {
	db, err := genji.Open(":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin(true)
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

	d, err := tx.QueryDocument("SELECT id, name, age FROM user WHERE name = ?", "foo")
	if err != nil {
		panic(err)
	}

	var u User
	err = document.StructScan(d, &u)
	if err != nil {
		panic(err)
	}

	fmt.Println(u)

	var id uint64
	var name string
	var age uint8

	err = document.Scan(d, &id, &name, &age)
	if err != nil {
		panic(err)
	}

	fmt.Println(id, name, age)

	err = tx.Commit()
	if err != nil {
		panic(err)
	}

	// Output: {10 foo 15 { }}
	// 10 foo 15
}

func TestQueryDocument(t *testing.T) {
	db, err := genji.Open(":memory:")
	assert.NoError(t, err)

	tx, err := db.Begin(true)
	assert.NoError(t, err)

	err = tx.Exec(`
			CREATE TABLE test;
			INSERT INTO test (a, b) VALUES (1, 'foo'), (2, 'bar')
		`)
	assert.NoError(t, err)
	assert.NoError(t, tx.Commit())

	t.Run("Should return the first document", func(t *testing.T) {
		var a int
		var b string

		r, err := db.QueryDocument("SELECT * FROM test")
		assert.NoError(t, err)
		err = document.Scan(r, &a, &b)
		assert.NoError(t, err)
		require.Equal(t, 1, a)
		require.Equal(t, "foo", b)

		tx, err := db.Begin(false)
		assert.NoError(t, err)
		defer tx.Rollback()

		r, err = tx.QueryDocument("SELECT * FROM test")
		assert.NoError(t, err)
		err = document.Scan(r, &a, &b)
		assert.NoError(t, err)
		require.Equal(t, 1, a)
		require.Equal(t, "foo", b)
	})

	t.Run("Should return an error if no document", func(t *testing.T) {
		r, err := db.QueryDocument("SELECT * FROM test WHERE a > 100")
		assert.ErrorIs(t, err, errs.ErrDocumentNotFound)
		require.Nil(t, r)

		tx, err := db.Begin(false)
		assert.NoError(t, err)
		defer tx.Rollback()
		r, err = tx.QueryDocument("SELECT * FROM test WHERE a > 100")
		assert.ErrorIs(t, err, errs.ErrDocumentNotFound)
		require.Nil(t, r)
	})
}

func TestPrepareThreadSafe(t *testing.T) {
	db, err := genji.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	err = db.Exec("CREATE TABLE test(a int unique, b text); INSERT INTO test(a, b) VALUES (1, 'a'), (2, 'a')")
	assert.NoError(t, err)

	stmt, err := db.Prepare("SELECT COUNT(a) FROM test WHERE a < ? GROUP BY b ORDER BY a DESC LIMIT 5")
	assert.NoError(t, err)

	g, _ := errgroup.WithContext(context.Background())

	for i := 1; i <= 3; i++ {
		arg := i
		g.Go(func() error {
			res, err := stmt.Query(arg)
			if err != nil {
				return err
			}
			defer res.Close()

			return res.Iterate(func(d types.Document) error {
				return nil
			})
		})
	}

	err = g.Wait()
	assert.NoError(t, err)
}

func BenchmarkSelect(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			db, err := genji.Open(":memory:")
			assert.NoError(b, err)

			err = db.Exec("CREATE TABLE foo")
			assert.NoError(b, err)

			for i := 0; i < size; i++ {
				err = db.Exec("INSERT INTO foo(a, b) VALUES (1, 2);")
				assert.NoError(b, err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				res, _ := db.Query("SELECT * FROM foo")
				res.Iterate(func(d types.Document) error { return nil })
			}
		})
	}
}

func BenchmarkSelectWhere(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			db, err := genji.Open(":memory:")
			assert.NoError(b, err)

			err = db.Exec("CREATE TABLE foo")
			assert.NoError(b, err)

			for i := 0; i < size; i++ {
				err = db.Exec("INSERT INTO foo(a, b) VALUES (1, 2);")
				assert.NoError(b, err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				res, _ := db.Query("SELECT b FROM foo WHERE a > 0")
				res.Iterate(func(d types.Document) error { return nil })
			}
		})
	}
}

func BenchmarkPreparedSelectWhere(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			db, err := genji.Open(":memory:")
			assert.NoError(b, err)

			err = db.Exec("CREATE TABLE foo")
			assert.NoError(b, err)

			for i := 0; i < size; i++ {
				err = db.Exec("INSERT INTO foo(a, b) VALUES (1, 2);")
				assert.NoError(b, err)
			}

			p, _ := db.Prepare("SELECT b FROM foo WHERE a > 0")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				res, _ := p.Query()
				res.Iterate(func(d types.Document) error { return nil })
			}
		})
	}
}

func BenchmarkSelectPk(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			db, err := genji.Open(":memory:")
			assert.NoError(b, err)

			err = db.Exec("CREATE TABLE foo(a INT PRIMARY KEY)")
			assert.NoError(b, err)

			for i := 0; i < size; i++ {
				err = db.Exec("INSERT INTO foo(a) VALUES (?)", i)
				assert.NoError(b, err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				res, _ := db.Query("SELECT * FROM foo WHERE a = ?", size-1)
				res.Iterate(func(d types.Document) error { return nil })
			}
		})
	}
}
