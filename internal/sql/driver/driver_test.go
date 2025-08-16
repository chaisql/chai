package driver_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/chaisql/chai"
	"github.com/stretchr/testify/require"
)

type rowtest struct {
	A int
	B string
	C bool
}

func TestDriver(t *testing.T) {
	db, err := sql.Open("chai", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	res, err := db.Exec("CREATE TABLE test(a INT, b TEXT, c BOOL)")
	require.NoError(t, err)
	n, err := res.RowsAffected()
	require.Error(t, err)
	require.EqualValues(t, 0, n)

	for i := 0; i < 10; i++ {
		_, err = db.Exec("INSERT INTO test (a, b, c) VALUES (?, ?, ?)", i, fmt.Sprintf("foo%d", i), i%2 == 0)
		require.NoError(t, err)
	}

	t.Run("Wildcard", func(t *testing.T) {
		rows, err := db.Query("SELECT * FROM test")
		require.NoError(t, err)
		defer rows.Close()

		var count int
		var rt rowtest
		for rows.Next() {
			err = rows.Scan(&rt.A, &rt.B, &rt.C)
			require.NoError(t, err)
			require.Equal(t, rowtest{count, fmt.Sprintf("foo%d", count), count%2 == 0}, rt)
			count++
		}

		require.NoError(t, rows.Err())
		require.Equal(t, 10, count)
	})

	t.Run("Multiple columns", func(t *testing.T) {
		rows, err := db.Query("SELECT a, c FROM test")
		require.NoError(t, err)
		defer rows.Close()

		var count int
		var a int
		var c bool
		for rows.Next() {
			err = rows.Scan(&a, &c)
			require.NoError(t, err)
			require.Equal(t, count, a)
			require.Equal(t, count%2 == 0, c)
			count++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 10, count)
	})

	t.Run("Multiple columns with ORDER BY", func(t *testing.T) {
		rows, err := db.Query("SELECT a, c FROM test ORDER BY a")
		require.NoError(t, err)
		defer rows.Close()

		var count int
		var a int
		var c bool
		for rows.Next() {
			err = rows.Scan(&a, &c)
			require.NoError(t, err)
			require.Equal(t, count, a)
			require.Equal(t, count%2 == 0, c)
			count++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 10, count)
	})

	t.Run("Wilcards with ORDER BY", func(t *testing.T) {
		rows, err := db.Query("SELECT * FROM test ORDER BY a")
		require.NoError(t, err)
		defer rows.Close()

		var count int
		var rt rowtest
		for rows.Next() {
			err = rows.Scan(&rt.A, &rt.B, &rt.C)
			require.NoError(t, err)
			require.Equal(t, rowtest{count, fmt.Sprintf("foo%d", count), count%2 == 0}, rt)
			count++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 10, count)
	})

	t.Run("Wilcards with ORDER BY and LIMIT", func(t *testing.T) {
		rows, err := db.Query("SELECT * FROM test ORDER BY a LIMIT 5")
		require.NoError(t, err)
		defer rows.Close()

		var count int
		var rt rowtest
		for rows.Next() {
			err = rows.Scan(&rt.A, &rt.B, &rt.C)
			require.NoError(t, err)
			require.Equal(t, rowtest{count, fmt.Sprintf("foo%d", count), count%2 == 0}, rt)
			count++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 5, count)
	})

	t.Run("Multiple columns and wildcards", func(t *testing.T) {
		rows, err := db.Query("SELECT a, a, *, b, c, * FROM test")
		require.NoError(t, err)
		defer rows.Close()

		var count int
		var a int
		var aa int
		var b string
		var c bool
		var dt1, dt2 rowtest
		for rows.Next() {
			err = rows.Scan(&a, &aa, &dt1.A, &dt1.B, &dt1.C, &b, &c, &dt2.A, &dt2.B, &dt2.C)
			require.NoError(t, err)
			require.Equal(t, count, a)
			require.Equal(t, fmt.Sprintf("foo%d", count), b)

			require.Equal(t, count%2 == 0, c)
			require.Equal(t, rowtest{count, fmt.Sprintf("foo%d", count), count%2 == 0}, dt1)
			require.Equal(t, rowtest{count, fmt.Sprintf("foo%d", count), count%2 == 0}, dt2)
			count++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 10, count)
	})

	t.Run("Params", func(t *testing.T) {
		rows, err := db.Query("SELECT a FROM test WHERE a = ?", 5)
		require.NoError(t, err)
		defer rows.Close()

		var count int
		var a int
		for rows.Next() {
			err = rows.Scan(&a)
			require.NoError(t, err)
			require.Equal(t, 5, a)
			count++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 1, count)
	})

	t.Run("Named Params", func(t *testing.T) {
		rows, err := db.Query("SELECT a FROM test WHERE a = $val", sql.Named("val", 5))
		require.NoError(t, err)
		defer rows.Close()

		var count int
		var a int
		for rows.Next() {
			err = rows.Scan(&a)
			require.NoError(t, err)
			require.Equal(t, 5, a)
			count++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 1, count)
	})

	t.Run("Transactions", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		rows, err := tx.Query("SELECT * FROM test")
		require.NoError(t, err)
		defer rows.Close()

		var count int
		var rt rowtest
		for rows.Next() {
			err = rows.Scan(&rt.A, &rt.B, &rt.C)
			require.NoError(t, err)
			require.Equal(t, rowtest{count, fmt.Sprintf("foo%d", count), count%2 == 0}, rt)
			count++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 10, count)
	})

	t.Run("Multiple queries", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT * FROM test;;;
			INSERT INTO test (a, b, c) VALUES (10, 'foo10', true);
			SELECT * FROM test;
		`)
		require.NoError(t, err)
		defer rows.Close()

		var count int
		var rt rowtest
		for rows.Next() {
			err = rows.Scan(&rt.A, &rt.B, &rt.C)
			require.NoError(t, err)
			require.Equal(t, rowtest{count, fmt.Sprintf("foo%d", count), count%2 == 0}, rt)
			count++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 11, count)
	})

	t.Run("Multiple queries in transaction", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		rows, err := tx.Query(`
			SELECT * FROM test;;;
			INSERT INTO test (a, b, c) VALUES (11, 'foo11', false);
			SELECT * FROM test;
		`)
		require.NoError(t, err)
		defer rows.Close()

		var count int
		var rt rowtest
		for rows.Next() {
			err = rows.Scan(&rt.A, &rt.B, &rt.C)
			require.NoError(t, err)
			require.Equal(t, rowtest{count, fmt.Sprintf("foo%d", count), count%2 == 0}, rt)
			count++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 12, count)
	})

	t.Run("Multiple queries in read only transaction", func(t *testing.T) {
		tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		_, err = tx.Query(`
			SELECT * FROM test;;;
			INSERT INTO test (a, b, c) VALUES (12, 13, 14);
			SELECT * FROM test;
		`)
		require.EqualError(t, err, "cannot increment sequence on read-only transaction")
	})
}

func TestDriverWithTimeValues(t *testing.T) {
	db, err := sql.Open("chai", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	now := time.Now().UTC().Truncate(time.Microsecond)
	_, err = db.Exec("CREATE TABLE test(a TIMESTAMP); INSERT INTO test (a) VALUES (?)", now)
	require.NoError(t, err)

	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	var tt time.Time
	err = tx.QueryRow(`SELECT a FROM test`).Scan(&tt)
	require.NoError(t, err)
	require.Equal(t, now, tt)
}
