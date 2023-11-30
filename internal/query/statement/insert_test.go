package statement_test

import (
	"bytes"
	"database/sql"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
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
		{"Values / Positional Params", "INSERT INTO test (a, b, c) VALUES (?, 'e', ?)", false, `[{"pk()":[1],"a":"d","b":"e","c":"f"}]`, []interface{}{"d", "f"}},
		{"Values / Named Params", "INSERT INTO test (a, b, c) VALUES ($d, 'e', $f)", false, `[{"pk()":[1],"a":"d","b":"e","c":"f"}]`, []interface{}{sql.Named("f", "f"), sql.Named("d", "d")}},
		{"Values / Invalid params", "INSERT INTO test (a, b, c) VALUES ('d', ?)", true, "", []interface{}{'e'}},
		{"Objects / Named Params", "INSERT INTO test VALUES {a: $a, b: 2.3, c: $c}", false, `[{"pk()":[1],"a":1,"b":2.3,"c":true}]`, []interface{}{sql.Named("c", true), sql.Named("a", 1)}},
		{"Objects / List ", "INSERT INTO test VALUES {a: [1, 2, 3]}", false, `[{"pk()":[1],"a":[1,2,3]}]`, nil},
		{"Select / same table", "INSERT INTO test SELECT * FROM test", true, ``, nil},
	}

	for _, test := range tests {
		testFn := func(withIndexes bool) func(t *testing.T) {
			return func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				err = db.Exec("CREATE TABLE test(a any, b any, c any)")
				assert.NoError(t, err)
				if withIndexes {
					err = db.Exec(`
						CREATE INDEX idx_a ON test (a);
						CREATE INDEX idx_b ON test (b);
						CREATE INDEX idx_c ON test (c);
					`)
					assert.NoError(t, err)
				}

				err = db.Exec(test.query, test.params...)
				if test.fails {
					assert.Error(t, err)
					return
				}
				assert.NoError(t, err)

				st, err := db.Query("SELECT pk(), * FROM test")
				assert.NoError(t, err)
				defer st.Close()

				var buf bytes.Buffer
				err = st.MarshalJSONTo(&buf)
				assert.NoError(t, err)
				require.JSONEq(t, test.expected, buf.String())
			}
		}

		t.Run("No Index/"+test.name, testFn(false))
		t.Run("With Index/"+test.name, testFn(true))
	}

	t.Run("with struct param", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		err = db.Exec("CREATE TABLE test")
		assert.NoError(t, err)

		type foo struct {
			A string
			B string `genji:"b-b"`
		}

		err = db.Exec("INSERT INTO test VALUES ?", &foo{A: "a", B: "b"})
		assert.NoError(t, err)
		res, err := db.Query("SELECT * FROM test")
		defer res.Close()

		assert.NoError(t, err)
		buf, err := res.MarshalJSON()
		assert.NoError(t, err)
		require.JSONEq(t, `[{"a": "a", "b-b": "b"}]`, string(buf))
	})

	t.Run("with RETURNING", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		err = db.Exec(`CREATE TABLE test`)
		assert.NoError(t, err)

		d, err := db.QueryRow(`insert into test (a) VALUES (1) RETURNING *, pk(), a AS A`)
		assert.NoError(t, err)
		testutil.RequireJSONEq(t, d, `{"a": 1, "pk()": [1], "A": 1}`)
	})

	t.Run("ensure rollback", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		err = db.Exec(`CREATE TABLE test(a int unique)`)
		assert.NoError(t, err)

		err = db.Exec(`insert into test (a) VALUES (1), (1)`)
		assert.Error(t, err)

		res, err := db.Query("SELECT * FROM test")
		assert.NoError(t, err)
		defer res.Close()

		testutil.RequireStreamEq(t, ``, res, false)
	})

	t.Run("with NEXT VALUE FOR", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		err = db.Exec(`CREATE SEQUENCE seq; CREATE TABLE test(a int, b int default NEXT VALUE FOR seq)`)
		assert.NoError(t, err)

		err = db.Exec(`insert into test (a) VALUES (1), (2), (3)`)
		assert.NoError(t, err)

		res, err := db.Query("SELECT * FROM test")
		assert.NoError(t, err)
		defer res.Close()

		var b bytes.Buffer
		err = res.MarshalJSONTo(&b)
		require.NoError(t, err)

		require.JSONEq(t, `
		[{"a": 1, "b": 1},
		{"a": 2, "b": 2},
		{"a": 3, "b": 3}]

		`, b.String())
	})
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
		{"No fields / No projection", `INSERT INTO foo SELECT * FROM bar`, false, `[{"pk()":[1], "a":1, "b":10}]`, nil},
		{"No fields / Projection", `INSERT INTO foo SELECT a FROM bar`, false, `[{"pk()":[1], "a":1}]`, nil},
		{"With fields / No Projection", `INSERT INTO foo (a, b) SELECT * FROM bar`, false, `[{"pk()":[1], "a":1, "b":10}]`, nil},
		{"With fields / Projection", `INSERT INTO foo (c, d) SELECT a, b FROM bar`, false, `[{"pk()":[1], "c":1, "d":10}]`, nil},
		{"Too many fields / No Projection", `INSERT INTO foo (c) SELECT * FROM bar`, true, ``, nil},
		{"Too many fields / Projection", `INSERT INTO foo (c, d) SELECT a, b, c FROM bar`, true, ``, nil},
		{"Too few fields / No Projection", `INSERT INTO foo (c, d, e) SELECT * FROM bar`, true, ``, nil},
		{"Too few fields / Projection", `INSERT INTO foo (c, d) SELECT a FROM bar`, true, ``, nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			assert.NoError(t, err)
			defer db.Close()

			err = db.Exec(`
				CREATE TABLE foo;
				CREATE TABLE bar;
				INSERT INTO bar (a, b) VALUES (1, 10)
			`)
			assert.NoError(t, err)

			err = db.Exec(test.query, test.params...)
			if test.fails {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			st, err := db.Query("SELECT pk(), * FROM foo")
			assert.NoError(t, err)
			defer st.Close()

			var buf bytes.Buffer
			err = st.MarshalJSONTo(&buf)
			assert.NoError(t, err)
			require.JSONEq(t, test.expected, buf.String())
		})
	}
}
