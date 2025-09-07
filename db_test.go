package chai_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	_ "github.com/chaisql/chai"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/stretchr/testify/require"
)

func ExampleTx() {
	db, err := sql.Open("chai", ":memory:")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()

	_, err = tx.Exec("CREATE TABLE IF NOT EXISTS user (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		panic(err)
	}

	_, err = tx.Exec("INSERT INTO user (id, name, age) VALUES ($1, $2, $3)", 10, "foo", 15)
	if err != nil {
		panic(err)
	}

	r := tx.QueryRow("SELECT id, name, age FROM user WHERE name = $1", "foo")

	var u User
	err = r.Scan(&u.ID, &u.Name, &u.Age)
	if err != nil {
		panic(err)
	}

	fmt.Println(u.ID, u.Name, u.Age)

	err = tx.Commit()
	if err != nil {
		panic(err)
	}

	// Output:
	// 10 foo 15
}

func TestOpen(t *testing.T) {
	dir, err := os.MkdirTemp("", "chai")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := sql.Open("chai", filepath.Join(dir, "testdb"))
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE tableA (a INTEGER UNIQUE NOT NULL, b DOUBLE PRIMARY KEY);
		CREATE TABLE tableB (a TEXT NOT NULL DEFAULT 'hello', PRIMARY KEY (a));
		CREATE TABLE tableC (a INTEGER PRIMARY KEY, b INTEGER);
		CREATE INDEX tableC_a_b_idx ON tableC(a, b);
		CREATE SEQUENCE seqD INCREMENT BY 10 CYCLE MINVALUE 100 NO MAXVALUE START 500;

		INSERT INTO tableB (a) VALUES (1);
		INSERT INTO tableC (a, b) VALUES (1, nextval('seqD'));
	`)
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)

	// ensure tables are loaded properly
	db, err = sql.Open("chai", filepath.Join(dir, "testdb"))
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()

	rows, err := conn.QueryContext(t.Context(), "SELECT * FROM __chai_catalog")
	require.NoError(t, err)

	want := []string{
		`{"name":"__chai_catalog", "namespace":1, "owner_table_columns":null, "owner_table_name":null, "rowid_sequence_name":null, "sql":"CREATE TABLE __chai_catalog (name TEXT NOT NULL, type TEXT NOT NULL, namespace BIGINT, sql TEXT, rowid_sequence_name TEXT, owner_table_name TEXT, owner_table_columns TEXT, CONSTRAINT __chai_catalog_pk PRIMARY KEY (name))", "type":"table"}`,
		`{"name":"__chai_sequence", "namespace":2, "owner_table_columns":null, "owner_table_name":null, "rowid_sequence_name":null, "sql":"CREATE TABLE __chai_sequence (name TEXT NOT NULL, seq BIGINT, CONSTRAINT __chai_sequence_pk PRIMARY KEY (name))", "type":"table"}`,
		`{"name":"__chai_store_seq", "namespace":null, "owner_table_columns":null, "owner_table_name":"__chai_catalog", "rowid_sequence_name":null, "sql":"CREATE SEQUENCE __chai_store_seq MAXVALUE 9223372036837998591 START WITH 10", "type":"sequence"}`,
		`{"name":"seqD", "namespace":null, "owner_table_columns":null, "owner_table_name":null, "rowid_sequence_name":null, "sql":"CREATE SEQUENCE seqD INCREMENT BY 10 MINVALUE 100 START WITH 500 CYCLE", "type":"sequence"}`,
		`{"name":"tableA", "namespace":10, "owner_table_columns":null, "owner_table_name":null, "rowid_sequence_name":null, "sql":"CREATE TABLE tableA (a INTEGER NOT NULL, b DOUBLE NOT NULL, CONSTRAINT tableA_a_unique UNIQUE (a), CONSTRAINT tableA_pk PRIMARY KEY (b))", "type":"table"}`,
		`{"name":"tableA_a_idx", "namespace":11, "owner_table_columns":"a", "owner_table_name":"tableA", "rowid_sequence_name":null, "sql":"CREATE UNIQUE INDEX tableA_a_idx ON tableA (a)", "type":"index"}`,
		`{"name":"tableB", "namespace":12, "owner_table_columns":null, "owner_table_name":null, "rowid_sequence_name":null, "sql":"CREATE TABLE tableB (a TEXT NOT NULL DEFAULT \"hello\", CONSTRAINT tableB_pk PRIMARY KEY (a))", "type":"table"}`,
		`{"name":"tableC", "namespace":13, "owner_table_columns":null, "owner_table_name":null, "rowid_sequence_name":null, "sql":"CREATE TABLE tableC (a INTEGER NOT NULL, b INTEGER, CONSTRAINT tableC_pk PRIMARY KEY (a))",  "type":"table"}`,
		`{"name":"tableC_a_b_idx", "namespace":14, "owner_table_columns":null, "owner_table_name":"tableC", "rowid_sequence_name":null, "sql":"CREATE INDEX tableC_a_b_idx ON tableC (a, b)", "type":"index"}`,
	}
	testutil.RequireJSONEq(t, rows, want...)

	rows, err = db.Query("SELECT * FROM tableB")
	require.NoError(t, err)
	testutil.RequireJSONEq(t, rows, `{"a": "1"}`)

	rows, err = db.Query("SELECT * FROM __chai_sequence LIMIT 1")
	require.NoError(t, err)
	testutil.RequireJSONEq(t, rows, `{"name":"__chai_store_seq", "seq":14}`)

	rows, err = db.Query("SELECT * FROM __chai_sequence LIMIT 1 OFFSET 1")
	require.NoError(t, err)
	testutil.RequireJSONEq(t, rows, `{"name": "seqD", "seq": 500}`)
}

func TestQuery(t *testing.T) {
	db, err := sql.Open("chai", ":memory:")
	require.NoError(t, err)
	require.NoError(t, err)

	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = tx.Exec(`
		CREATE TABLE test(a INTEGER PRIMARY KEY, b TEXT NOT NULL);
		INSERT INTO test (a, b) VALUES (1, 'foo'), (2, 'bar')
	`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	t.Run("Should return the first row", func(t *testing.T) {
		var a int
		var b string

		err := db.QueryRow("SELECT * FROM test").Scan(&a, &b)
		require.NoError(t, err)
		require.Equal(t, 1, a)
		require.Equal(t, "foo", b)

		tx, err := db.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.QueryRow("SELECT * FROM test").Scan(&a, &b)
		require.NoError(t, err)
		require.Equal(t, 1, a)
		require.Equal(t, "foo", b)
	})

	t.Run("Should return an error if no row", func(t *testing.T) {
		var a int
		var b string
		err := db.QueryRow("SELECT * FROM test WHERE a > 100").Scan(&a, &b)
		require.ErrorIs(t, err, sql.ErrNoRows)
	})
}

func TestIterateDeepCopy(t *testing.T) {
	db, err := sql.Open("chai", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
	CREATE TABLE foo (
		a integer primary key,
		b text not null
	);

	INSERT INTO foo (a, b) VALUES
		(1, 'sample text 1'),
		(2, 'sample text 2');
	`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT * FROM foo ORDER BY a DESC`)
	require.NoError(t, err)

	type item struct {
		A int
		B string
	}

	var items []*item
	for rows.Next() {
		var i item
		err := rows.Scan(&i.A, &i.B)
		require.NoError(t, err)

		items = append(items, &i)
	}
	require.NoError(t, err)

	require.Equal(t, len(items), 2)
	require.Equal(t, &item{A: 2, B: "sample text 2"}, items[0])
	require.Equal(t, &item{A: 1, B: "sample text 1"}, items[1])
}

func TestWWConcurrency(t *testing.T) {
	db, err := sql.Open("chai", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
	CREATE TABLE foo (
		a integer primary key,
		b text not null
	);`)
	require.NoError(t, err)

	iterations := 100
	concurrency := 8

	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := range concurrency {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_, err := db.Exec(`INSERT INTO foo (a, b) VALUES ($1, $2)`, id*1000+j, fmt.Sprintf("sample text %d", j))
				require.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()
}

func TestRWConcurrency(t *testing.T) {
	db, err := sql.Open("chai", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
	CREATE TABLE foo (
		a integer primary key,
		b text not null
	);

	-- insert sample data
	INSERT INTO foo(a, b) VALUES (1000000, 'sample text 1000000');
	`)
	require.NoError(t, err)

	readers := 8
	writers := 4
	iterations := 100

	var wg sync.WaitGroup
	wg.Add(readers + writers)

	for range readers {
		go func() {
			defer wg.Done()
			for range iterations {
				var a int
				var b string
				err := db.QueryRow(`SELECT * FROM foo WHERE a = 1000000`).Scan(&a, &b)
				require.NoError(t, err)
				require.Equal(t, 1000000, a)
				require.Equal(t, "sample text 1000000", b)
			}
		}()
	}

	for i := range writers {
		go func(id int) {
			defer wg.Done()
			for j := range iterations {
				_, err := db.Exec(`INSERT INTO foo (a, b) VALUES ($1, $2)`, id*1000+j, fmt.Sprintf("sample text %d", j))
				require.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()
}

func TestOrderByConcurrency(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("chai", dir)
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec(`
	CREATE TABLE foo (
		a integer primary key,
		b int not null
	);`)
	require.NoError(t, err)

	for i := range 10_000 {
		_, err := tx.Exec(`INSERT INTO foo (a, b) VALUES ($1, $2)`, i, i)
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())

	iterations := 10
	concurrency := 8

	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := range concurrency {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				var desc string
				if j%2 == 0 {
					desc = "DESC"
				} else {
					desc = "ASC"
				}
				func() {
					rows, err := db.Query(`SELECT * FROM foo ORDER BY b ` + desc)
					require.NoError(t, err)
					defer rows.Close()

					var count int
					for rows.Next() {
						var a int
						var b int
						err := rows.Scan(&a, &b)
						require.NoError(t, err)
						if desc == "ASC" {
							require.Equal(t, count, a)
							require.Equal(t, count, b)
						} else {
							require.Equal(t, 9_999-count, a)
							require.Equal(t, 9_999-count, b)
						}
						count++
					}
					require.NoError(t, rows.Err())
					require.Equal(t, 10_000, count)
				}()
			}
		}(i)
	}

	wg.Wait()
}

func BenchmarkOrderBy(b *testing.B) {
	dir := b.TempDir()
	db, err := sql.Open("chai", dir)
	require.NoError(b, err)
	defer db.Close()

	tx, err := db.Begin()
	require.NoError(b, err)
	_, err = tx.Exec(`
	CREATE TABLE foo (
		a integer primary key,
		b int not null
	);`)
	require.NoError(b, err)

	for i := range 10_000 {
		_, err := tx.Exec(`INSERT INTO foo (a, b) VALUES ($1, $2)`, i, i)
		require.NoError(b, err)
	}
	require.NoError(b, tx.Commit())

	b.ResetTimer()
	for b.Loop() {
		rows, err := db.Query(`SELECT * FROM foo ORDER BY b`)
		require.NoError(b, err)

		for rows.Next() {
			var a int
			var b int
			_ = rows.Scan(&a, &b)
		}
	}
}
