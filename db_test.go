package chai_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/chaisql/chai"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/testutil/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func ExampleTx() {
	db, err := chai.Open(":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin(true)
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()

	err = tx.Exec("CREATE TABLE IF NOT EXISTS user (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		panic(err)
	}

	err = tx.Exec("INSERT INTO user (id, name, age) VALUES (?, ?, ?)", 10, "foo", 15)
	if err != nil {
		panic(err)
	}

	r, err := tx.QueryRow("SELECT id, name, age FROM user WHERE name = ?", "foo")
	if err != nil {
		panic(fmt.Sprintf("%+v", err))
	}

	var u User
	err = r.StructScan(&u)
	if err != nil {
		panic(err)
	}

	fmt.Println(u)

	var id uint64
	var name string
	var age uint8

	err = r.Scan(&id, &name, &age)
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

func TestOpen(t *testing.T) {
	dir, err := os.MkdirTemp("", "chai")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := chai.Open(filepath.Join(dir, "testdb"))
	assert.NoError(t, err)

	err = db.Exec(`
		CREATE TABLE tableA (a INTEGER UNIQUE NOT NULL, b DOUBLE PRIMARY KEY);
		CREATE TABLE tableB (a TEXT NOT NULL DEFAULT 'hello', PRIMARY KEY (a));
		CREATE TABLE tableC (a INTEGER, b INTEGER);
		CREATE INDEX tableC_a_b_idx ON tableC(a, b);
		CREATE SEQUENCE seqD INCREMENT BY 10 CYCLE MINVALUE 100 NO MAXVALUE START 500;

		INSERT INTO tableB (a) VALUES (1);
		INSERT INTO tableC (a, b) VALUES (1, NEXT VALUE FOR seqD);
	`)
	assert.NoError(t, err)

	err = db.Close()
	assert.NoError(t, err)

	// ensure tables are loaded properly
	db, err = chai.Open(filepath.Join(dir, "testdb"))
	assert.NoError(t, err)
	defer db.Close()

	res1, err := db.Query("SELECT * FROM __chai_catalog")
	assert.NoError(t, err)
	defer res1.Close()

	var count int
	want := []string{
		`{"name":"__chai_catalog", "namespace":1, "owner_table_columns":null, "owner_table_name":null, "rowid_sequence_name":null, "sql":"CREATE TABLE __chai_catalog (name TEXT NOT NULL, type TEXT NOT NULL, namespace BIGINT, sql TEXT, rowid_sequence_name TEXT, owner_table_name TEXT, owner_table_columns TEXT, CONSTRAINT __chai_catalog_pk PRIMARY KEY (name))", "type":"table"}`,
		`{"name":"__chai_sequence", "namespace":2, "owner_table_columns":null, "owner_table_name":null, "rowid_sequence_name":null, "sql":"CREATE TABLE __chai_sequence (name TEXT NOT NULL, seq BIGINT, CONSTRAINT __chai_sequence_pk PRIMARY KEY (name))", "type":"table"}`,
		`{"name":"__chai_store_seq", "namespace":null, "owner_table_columns":null, "owner_table_name":"__chai_catalog", "rowid_sequence_name":null, "sql":"CREATE SEQUENCE __chai_store_seq MAXVALUE 9223372036837998591 START WITH 10 CACHE 0", "type":"sequence"}`,
		`{"name":"seqD", "namespace":null, "owner_table_columns":null, "owner_table_name":null, "rowid_sequence_name":null, "sql":"CREATE SEQUENCE seqD INCREMENT BY 10 MINVALUE 100 START WITH 500 CYCLE", "type":"sequence"}`,
		`{"name":"tableA", "namespace":10, "owner_table_columns":null, "owner_table_name":null, "rowid_sequence_name":null, "sql":"CREATE TABLE tableA (a INTEGER NOT NULL, b DOUBLE NOT NULL, CONSTRAINT tableA_a_unique UNIQUE (a), CONSTRAINT tableA_pk PRIMARY KEY (b))", "type":"table"}`,
		`{"name":"tableA_a_idx", "namespace":11, "owner_table_columns":"a", "owner_table_name":"tableA", "rowid_sequence_name":null, "sql":"CREATE UNIQUE INDEX tableA_a_idx ON tableA (a)", "type":"index"}`,
		`{"name":"tableB", "namespace":12, "owner_table_columns":null, "owner_table_name":null, "rowid_sequence_name":null, "sql":"CREATE TABLE tableB (a TEXT NOT NULL DEFAULT \"hello\", CONSTRAINT tableB_pk PRIMARY KEY (a))", "type":"table"}`,
		`{"name":"tableC", "namespace":13, "owner_table_columns":null, "owner_table_name":null, "rowid_sequence_name":"tableC_seq", "sql":"CREATE TABLE tableC (a INTEGER, b INTEGER)",  "type":"table"}`,
		`{"name":"tableC_a_b_idx", "namespace":14, "owner_table_columns":null, "owner_table_name":"tableC", "rowid_sequence_name":null, "sql":"CREATE INDEX tableC_a_b_idx ON tableC (a, b)", "type":"index"}`,
		`{"name":"tableC_seq", "namespace":null, "owner_table_columns":null, "owner_table_name":"tableC", "rowid_sequence_name":null, "sql":"CREATE SEQUENCE tableC_seq CACHE 64", "type":"sequence"}`,
	}
	err = res1.Iterate(func(r *chai.Row) error {
		count++
		if count > len(want) {
			return fmt.Errorf("more than %d relations", len(want))
		}

		testutil.RequireJSONEq(t, r, want[count-1])
		return nil
	})
	assert.NoError(t, err)

	d, err := db.QueryRow("SELECT * FROM tableB")
	assert.NoError(t, err)
	testutil.RequireJSONEq(t, d, `{"a": "1"}`)

	d, err = db.QueryRow("SELECT * FROM __chai_sequence")
	assert.NoError(t, err)
	testutil.RequireJSONEq(t, d, `{"name":"__chai_store_seq", "seq":14}`)

	d, err = db.QueryRow("SELECT * FROM __chai_sequence OFFSET 1")
	assert.NoError(t, err)
	testutil.RequireJSONEq(t, d, `{"name": "seqD", "seq": 500}`)
}

func TestQueryRow(t *testing.T) {
	db, err := chai.Open(":memory:")
	assert.NoError(t, err)
	require.NoError(t, err)

	tx, err := db.Begin(true)
	assert.NoError(t, err)

	err = tx.Exec(`
			CREATE TABLE test(a INTEGER PRIMARY KEY, b TEXT NOT NULL);
			INSERT INTO test (a, b) VALUES (1, 'foo'), (2, 'bar')
		`)
	assert.NoError(t, err)
	assert.NoError(t, tx.Commit())

	t.Run("Should return the first row", func(t *testing.T) {
		var a int
		var b string

		r, err := db.QueryRow("SELECT * FROM test")
		assert.NoError(t, err)
		err = r.Scan(&a, &b)
		assert.NoError(t, err)
		require.Equal(t, 1, a)
		require.Equal(t, "foo", b)

		tx, err := db.Begin(false)
		assert.NoError(t, err)
		defer tx.Rollback()

		r, err = tx.QueryRow("SELECT * FROM test")
		assert.NoError(t, err)
		err = r.Scan(&a, &b)
		assert.NoError(t, err)
		require.Equal(t, 1, a)
		require.Equal(t, "foo", b)
	})

	t.Run("Should return an error if no row", func(t *testing.T) {
		r, err := db.QueryRow("SELECT * FROM test WHERE a > 100")
		require.True(t, chai.IsNotFoundError(err))
		require.Nil(t, r)

		tx, err := db.Begin(false)
		assert.NoError(t, err)
		defer tx.Rollback()
		r, err = tx.QueryRow("SELECT * FROM test WHERE a > 100")
		require.True(t, chai.IsNotFoundError(err))
		require.Nil(t, r)
	})
}

func TestPrepareThreadSafe(t *testing.T) {
	db, err := chai.Open(":memory:")
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

			return res.Iterate(func(d *chai.Row) error {
				return nil
			})
		})
	}

	err = g.Wait()
	assert.NoError(t, err)
}

func TestIterateDeepCopy(t *testing.T) {
	db, err := chai.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	err = db.Exec(`
	CREATE TABLE foo (
		a integer primary key,
		b text not null
	);

	INSERT INTO foo (a, b) VALUES
		(1, 'sample text 1'),
		(2, 'sample text 2');
	`)
	assert.NoError(t, err)

	res, err := db.Query(`SELECT * FROM foo ORDER BY a DESC`)
	assert.NoError(t, err)

	type item struct {
		A int
		B string
	}

	var items []*item
	err = res.Iterate(func(r *chai.Row) error {
		var i item
		err := r.StructScan(&i)
		assert.NoError(t, err)

		items = append(items, &i)
		return nil
	})
	assert.NoError(t, err)

	require.Equal(t, len(items), 2)
	require.Equal(t, &item{A: 2, B: "sample text 2"}, items[0])
	require.Equal(t, &item{A: 1, B: "sample text 1"}, items[1])
}

func BenchmarkSelect(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			db, err := chai.Open(":memory:")
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
				res.Iterate(func(d *chai.Row) error { return nil })
			}
		})
	}
}

func BenchmarkSelectWhere(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			db, err := chai.Open(":memory:")
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
				res.Iterate(func(d *chai.Row) error { return nil })
			}
		})
	}
}

func BenchmarkPreparedSelectWhere(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			db, err := chai.Open(":memory:")
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
				res.Iterate(func(d *chai.Row) error { return nil })
			}
		})
	}
}

func BenchmarkSelectPk(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			db, err := chai.Open(":memory:")
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
				res.Iterate(func(d *chai.Row) error { return nil })
			}
		})
	}
}

func BenchmarkInsert(b *testing.B) {
	db, err := chai.Open(b.TempDir())
	assert.NoError(b, err)
	defer db.Close()

	err = db.Exec("CREATE TABLE foo(a INT)")
	assert.NoError(b, err)

	stmt, err := db.Prepare("INSERT INTO foo(a) VALUES (?)")
	assert.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			stmt.Exec(j)
		}
	}
}
