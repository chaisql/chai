package driver

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/stretchr/testify/require"
)

type doctest struct {
	A int
	B []int
	C struct{ Foo string }
}

type foo struct{ Foo string }

func TestDriver(t *testing.T) {
	db, err := sql.Open("genji", ":memory:")
	assert.NoError(t, err)
	defer db.Close()

	res, err := db.Exec("CREATE TABLE test")
	assert.NoError(t, err)
	n, err := res.RowsAffected()
	assert.Error(t, err)
	require.EqualValues(t, 0, n)

	for i := 0; i < 10; i++ {
		_, err = db.Exec("INSERT INTO test (a, b, c) VALUES (?, ?, ?)", i, []int{i + 1, i + 2, i + 3}, &foo{Foo: "bar"})
		assert.NoError(t, err)
	}

	t.Run("Wildcard", func(t *testing.T) {
		rows, err := db.Query("SELECT * FROM test")
		assert.NoError(t, err)
		defer rows.Close()

		var count int
		var dt doctest
		for rows.Next() {
			err = rows.Scan(Scanner(&dt))
			assert.NoError(t, err)
			require.Equal(t, doctest{count, []int{count + 1, count + 2, count + 3}, foo{Foo: "bar"}}, dt)
			count++
		}

		assert.NoError(t, rows.Err())
		require.Equal(t, 10, count)
	})

	t.Run("Multiple fields", func(t *testing.T) {
		rows, err := db.Query("SELECT a, c FROM test")
		assert.NoError(t, err)
		defer rows.Close()

		var count int
		var a int
		var c foo
		for rows.Next() {
			err = rows.Scan(&a, Scanner(&c))
			assert.NoError(t, err)
			require.Equal(t, count, a)
			require.Equal(t, foo{Foo: "bar"}, c)
			count++
		}
		assert.NoError(t, rows.Err())
		require.Equal(t, 10, count)
	})

	t.Run("Multiple fields with ORDER BY", func(t *testing.T) {
		rows, err := db.Query("SELECT a, c FROM test ORDER BY a")
		assert.NoError(t, err)
		defer rows.Close()

		var count int
		var a int
		var c foo
		for rows.Next() {
			err = rows.Scan(&a, Scanner(&c))
			assert.NoError(t, err)
			require.Equal(t, count, a)
			require.Equal(t, foo{Foo: "bar"}, c)
			count++
		}
		assert.NoError(t, rows.Err())
		require.Equal(t, 10, count)
	})

	t.Run("Wilcards with ORDER BY", func(t *testing.T) {
		rows, err := db.Query("SELECT * FROM test ORDER BY a")
		assert.NoError(t, err)
		defer rows.Close()

		var count int
		var dt doctest
		for rows.Next() {
			err = rows.Scan(Scanner(&dt))
			assert.NoError(t, err)
			require.Equal(t, doctest{count, []int{count + 1, count + 2, count + 3}, foo{Foo: "bar"}}, dt)
			count++
		}
		assert.NoError(t, rows.Err())
		require.Equal(t, 10, count)
	})

	t.Run("Wilcards with ORDER BY and LIMIT", func(t *testing.T) {
		rows, err := db.Query("SELECT * FROM test ORDER BY a LIMIT 5")
		assert.NoError(t, err)
		defer rows.Close()

		var count int
		var dt doctest
		for rows.Next() {
			err = rows.Scan(Scanner(&dt))
			assert.NoError(t, err)
			require.Equal(t, doctest{count, []int{count + 1, count + 2, count + 3}, foo{Foo: "bar"}}, dt)
			count++
		}
		assert.NoError(t, rows.Err())
		require.Equal(t, 5, count)
	})

	t.Run("Multiple fields and wildcards", func(t *testing.T) {
		rows, err := db.Query("SELECT a, a, *, b, c, * FROM test")
		assert.NoError(t, err)
		defer rows.Close()

		var count int
		var a int
		var aa int
		var b []float32
		var c foo
		var dt1, dt2 doctest
		for rows.Next() {
			err = rows.Scan(&a, Scanner(&aa), Scanner(&dt1), Scanner(&b), Scanner(&c), Scanner(&dt2))
			assert.NoError(t, err)
			require.Equal(t, count, a)
			require.Equal(t, []float32{float32(count + 1), float32(count + 2), float32(count + 3)}, b)
			require.Equal(t, foo{Foo: "bar"}, c)
			require.Equal(t, doctest{count, []int{count + 1, count + 2, count + 3}, foo{Foo: "bar"}}, dt1)
			require.Equal(t, doctest{count, []int{count + 1, count + 2, count + 3}, foo{Foo: "bar"}}, dt2)
			count++
		}
		assert.NoError(t, rows.Err())
		require.Equal(t, 10, count)
	})

	t.Run("Params", func(t *testing.T) {
		rows, err := db.Query("SELECT a FROM test WHERE a = ?", 5)
		assert.NoError(t, err)
		defer rows.Close()

		var count int
		var a int
		for rows.Next() {
			err = rows.Scan(&a)
			assert.NoError(t, err)
			require.Equal(t, 5, a)
			count++
		}
		assert.NoError(t, rows.Err())
		require.Equal(t, 1, count)
	})

	t.Run("Named Params", func(t *testing.T) {
		rows, err := db.Query("SELECT a FROM test WHERE a = $val", sql.Named("val", 5))
		assert.NoError(t, err)
		defer rows.Close()

		var count int
		var a int
		for rows.Next() {
			err = rows.Scan(&a)
			assert.NoError(t, err)
			require.Equal(t, 5, a)
			count++
		}
		assert.NoError(t, rows.Err())
		require.Equal(t, 1, count)
	})

	t.Run("Transactions", func(t *testing.T) {
		tx, err := db.Begin()
		assert.NoError(t, err)
		defer tx.Rollback()

		rows, err := tx.Query("SELECT * FROM test")
		assert.NoError(t, err)
		defer rows.Close()

		var count int
		var dt doctest
		for rows.Next() {
			err = rows.Scan(Scanner(&dt))
			assert.NoError(t, err)
			require.Equal(t, doctest{count, []int{count + 1, count + 2, count + 3}, foo{Foo: "bar"}}, dt)
			count++
		}
		assert.NoError(t, rows.Err())
		require.Equal(t, 10, count)
	})

	t.Run("Multiple queries", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT * FROM test;;;
			INSERT INTO test (a, b, c) VALUES (10, [11, 12, 13], {foo: "bar"});
			SELECT * FROM test;
		`)
		assert.NoError(t, err)
		defer rows.Close()

		var count int
		var dt doctest
		for rows.Next() {
			err = rows.Scan(Scanner(&dt))
			assert.NoError(t, err)
			require.Equal(t, doctest{count, []int{count + 1, count + 2, count + 3}, foo{Foo: "bar"}}, dt)
			count++
		}
		assert.NoError(t, rows.Err())
		require.Equal(t, 11, count)
	})

	t.Run("Multiple queries in transaction", func(t *testing.T) {
		tx, err := db.Begin()
		assert.NoError(t, err)
		defer tx.Rollback()

		rows, err := tx.Query(`
			SELECT * FROM test;;;
			INSERT INTO test (a, b, c) VALUES (11, [12, 13, 14], {foo: "bar"});
			SELECT * FROM test;
		`)
		assert.NoError(t, err)
		defer rows.Close()

		var count int
		var dt doctest
		for rows.Next() {
			err = rows.Scan(Scanner(&dt))
			assert.NoError(t, err)
			require.Equal(t, doctest{count, []int{count + 1, count + 2, count + 3}, foo{Foo: "bar"}}, dt)
			count++
		}
		assert.NoError(t, rows.Err())
		require.Equal(t, 12, count)
	})

	t.Run("Multiple queries in read only transaction", func(t *testing.T) {
		tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
		assert.NoError(t, err)
		defer tx.Rollback()

		_, err = tx.Query(`
			SELECT * FROM test;;;
			INSERT INTO test (a, b, c) VALUES (12, 13, 14);
			SELECT * FROM test;
		`)
		require.EqualError(t, err, "cannot increment sequence on read-only transaction")
	})
}

func TestDriverWithTimeValues(t *testing.T) {
	db, err := sql.Open("genji", ":memory:")
	assert.NoError(t, err)
	defer db.Close()

	now := time.Now().UTC()
	_, err = db.Exec("CREATE TABLE test; INSERT INTO test (a) VALUES (?)", now)
	assert.NoError(t, err)

	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	assert.NoError(t, err)
	defer tx.Rollback()

	var tt time.Time
	err = tx.QueryRow(`SELECT a FROM test`).Scan(Scanner(&tt))
	assert.NoError(t, err)
	require.Equal(t, now, tt)
}
