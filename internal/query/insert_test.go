package query_test

import (
	"bytes"
	"database/sql"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestInsertStmt(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		fails    bool
		expected string
		params   []interface{}
	}{
		{"Values / Positional Params", "INSERT INTO test (a, b, c) VALUES (?, 'e', ?)", false, `[{"pk()":1,"a":"d","b":"e","c":"f"}]`, []interface{}{"d", "f"}},
		{"Values / Named Params", "INSERT INTO test (a, b, c) VALUES ($d, 'e', $f)", false, `[{"pk()":1,"a":"d","b":"e","c":"f"}]`, []interface{}{sql.Named("f", "f"), sql.Named("d", "d")}},
		{"Values / Invalid params", "INSERT INTO test (a, b, c) VALUES ('d', ?)", true, "", []interface{}{'e'}},
		{"Documents / Named Params", "INSERT INTO test VALUES {a: $a, b: 2.3, c: $c}", false, `[{"pk()":1,"a":1,"b":2.3,"c":true}]`, []interface{}{sql.Named("c", true), sql.Named("a", 1)}},
		{"Documents / List ", "INSERT INTO test VALUES {a: [1, 2, 3]}", false, `[{"pk()":1,"a":[1,2,3]}]`, nil},
	}

	for _, test := range tests {
		testFn := func(withIndexes bool) func(t *testing.T) {
			return func(t *testing.T) {
				db, err := genji.Open(":memory:")
				require.NoError(t, err)
				defer db.Close()

				err = db.Exec("CREATE TABLE test")
				require.NoError(t, err)
				if withIndexes {
					err = db.Exec(`
						CREATE INDEX idx_a ON test (a);
						CREATE INDEX idx_b ON test (b);
						CREATE INDEX idx_c ON test (c);
					`)
					require.NoError(t, err)
				}

				err = db.Exec(test.query, test.params...)
				if test.fails {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)

				st, err := db.Query("SELECT pk(), * FROM test")
				require.NoError(t, err)
				defer st.Close()

				var buf bytes.Buffer
				err = testutil.IteratorToJSONArray(&buf, st)
				require.NoError(t, err)
				require.JSONEq(t, test.expected, buf.String())
			}
		}

		t.Run("No Index/"+test.name, testFn(false))
		t.Run("With Index/"+test.name, testFn(true))
	}

	t.Run("with struct param", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		err = db.Exec("CREATE TABLE test")
		require.NoError(t, err)

		type foo struct {
			A string
			B string `genji:"b-b"`
		}

		err = db.Exec("INSERT INTO test VALUES ?", &foo{A: "a", B: "b"})
		require.NoError(t, err)
		res, err := db.Query("SELECT * FROM test")
		defer res.Close()

		require.NoError(t, err)
		var buf bytes.Buffer
		err = testutil.IteratorToJSONArray(&buf, res)
		require.NoError(t, err)
		require.JSONEq(t, `[{"a": "a", "b-b": "b"}]`, buf.String())
	})

	t.Run("with RETURNING", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		err = db.Exec(`CREATE TABLE test`)
		require.NoError(t, err)

		d, err := db.QueryDocument(`insert into test (a) VALUES (1) RETURNING *, pk(), a AS A`)
		require.NoError(t, err)
		testutil.RequireDocJSONEq(t, d, `{"a": 1, "pk()": 1, "A": 1}`)
	})

	// t.Run("without RETURNING", func(t *testing.T) {
	// 	db, err := genji.Open(":memory:")
	// 	require.NoError(t, err)
	// 	defer db.Close()

	// 	err = db.Exec(`CREATE TABLE test`)
	// 	require.NoError(t, err)

	// 	_, err = db.QueryDocument(`insert into test (a) VALUES (1)`)
	// 	require.Equal(t, errs.ErrDocumentNotFound, err)
	// })
}

func TestInsertSelect(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		fails    bool
		expected string
		params   []interface{}
	}{
		{"Same table", `INSERT INTO foo SELECT * FROM foo`, true, ``, nil},
		{"No fields / No projection", `INSERT INTO foo SELECT * FROM bar`, false, `[{"pk()":1, "a":1, "b":10}]`, nil},
		{"No fields / Projection", `INSERT INTO foo SELECT a FROM bar`, false, `[{"pk()":1, "a":1}]`, nil},
		{"With fields / No Projection", `INSERT INTO foo (a, b) SELECT * FROM bar`, false, `[{"pk()":1, "a":1, "b":10}]`, nil},
		{"With fields / Projection", `INSERT INTO foo (c, d) SELECT a, b FROM bar`, false, `[{"pk()":1, "c":1, "d":10}]`, nil},
		{"Too many fields / No Projection", `INSERT INTO foo (c) SELECT * FROM bar`, true, ``, nil},
		{"Too many fields / Projection", `INSERT INTO foo (c, d) SELECT a, b, c FROM bar`, true, ``, nil},
		{"Too few fields / No Projection", `INSERT INTO foo (c, d, e) SELECT * FROM bar`, true, ``, nil},
		{"Too few fields / Projection", `INSERT INTO foo (c, d) SELECT a FROM bar`, true, ``, nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec(`
				CREATE TABLE foo;
				CREATE TABLE bar;
				INSERT INTO bar (a, b) VALUES (1, 10)
			`)
			require.NoError(t, err)

			err = db.Exec(test.query, test.params...)
			if test.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			st, err := db.Query("SELECT pk(), * FROM foo")
			require.NoError(t, err)
			defer st.Close()

			var buf bytes.Buffer
			err = testutil.IteratorToJSONArray(&buf, st)
			require.NoError(t, err)
			require.JSONEq(t, test.expected, buf.String())
		})
	}
}
