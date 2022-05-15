package genji_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/testutil"
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

func TestOpen(t *testing.T) {
	dir, err := ioutil.TempDir("", "genji")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := genji.Open(filepath.Join(dir, "testdb"))
	assert.NoError(t, err)

	err = db.Exec(`
		CREATE TABLE tableA (a INTEGER UNIQUE NOT NULL, b (c (d DOUBLE PRIMARY KEY)));
		CREATE TABLE tableB (a TEXT NOT NULL DEFAULT 'hello', PRIMARY KEY (a));
		CREATE TABLE tableC;
		CREATE INDEX tableC_a_b_idx ON tableC(a, b);
		CREATE SEQUENCE seqD INCREMENT BY 10 CYCLE MINVALUE 100 NO MAXVALUE START 500;

		INSERT INTO tableB (a) VALUES (1);
		INSERT INTO tableC (a, b) VALUES (1, NEXT VALUE FOR seqD);
	`)
	assert.NoError(t, err)

	err = db.Close()
	assert.NoError(t, err)

	// ensure tables are loaded properly
	db, err = genji.Open(filepath.Join(dir, "testdb"))
	assert.NoError(t, err)
	defer db.Close()

	res1, err := db.Query("SELECT * FROM __genji_catalog")
	assert.NoError(t, err)
	defer res1.Close()

	var count int
	want := []string{
		`{"name":"__genji_sequence", "sql":"CREATE TABLE __genji_sequence (name TEXT NOT NULL, seq INTEGER, CONSTRAINT __genji_sequence_pk PRIMARY KEY (name))", "namespace":2, "type":"table"}`,
		`{"name":"__genji_store_seq", "owner":{"table_name":"__genji_catalog"}, "sql":"CREATE SEQUENCE __genji_store_seq MAXVALUE 4294967295 START WITH 101 CACHE 0", "type":"sequence"}`,
		`{"name":"seqD", "sql":"CREATE SEQUENCE seqD INCREMENT BY 10 MINVALUE 100 START WITH 500 CYCLE", "type":"sequence"}`,
		`{"name":"tableA", "sql":"CREATE TABLE tableA (a INTEGER NOT NULL, b (c (d DOUBLE NOT NULL)), CONSTRAINT tableA_a_unique UNIQUE (a), CONSTRAINT tableA_pk PRIMARY KEY (b.c.d))", "namespace":101, "type":"table"}`,
		`{"name":"tableA_a_idx", "owner":{"table_name":"tableA", "paths":["a"]}, "sql":"CREATE UNIQUE INDEX tableA_a_idx ON tableA (a)", "namespace":102, "type":"index"}`,
		`{"name":"tableB", "sql":"CREATE TABLE tableB (a TEXT NOT NULL DEFAULT \"hello\", CONSTRAINT tableB_pk PRIMARY KEY (a))", "namespace":103, "type":"table"}`,
		`{"name":"tableC", "docid_sequence_name":"tableC_seq", "sql":"CREATE TABLE tableC (...)", "namespace":104, "type":"table"}`,
		`{"name":"tableC_a_b_idx", "owner":{"table_name":"tableC"}, "sql":"CREATE INDEX tableC_a_b_idx ON tableC (a, b)", "namespace":105, "type":"index"}`,
		`{"name":"tableC_seq", "owner":{"table_name":"tableC"}, "sql":"CREATE SEQUENCE tableC_seq CACHE 64", "type":"sequence"}`,
	}
	err = res1.Iterate(func(d types.Document) error {
		count++
		if count > len(want) {
			return fmt.Errorf("more than %d relations", len(want))
		}

		testutil.RequireDocJSONEq(t, d, want[count-1])
		return nil
	})
	assert.NoError(t, err)

	d, err := db.QueryDocument("SELECT * FROM tableB")
	assert.NoError(t, err)
	testutil.RequireDocJSONEq(t, d, `{"a": "1"}`)

	d, err = db.QueryDocument("SELECT * FROM __genji_sequence")
	assert.NoError(t, err)
	testutil.RequireDocJSONEq(t, d, `{"name":"__genji_store_seq", "seq":105}`)

	d, err = db.QueryDocument("SELECT * FROM __genji_sequence OFFSET 1")
	assert.NoError(t, err)
	testutil.RequireDocJSONEq(t, d, `{"name": "seqD", "seq": 500}`)
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
