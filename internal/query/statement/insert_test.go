package statement_test

import (
	"bytes"
	"database/sql"
	"testing"

	"github.com/chaisql/chai"
	"github.com/chaisql/chai/internal/testutil"
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
		{"Values / Positional Params", "INSERT INTO test (a, b, c) VALUES (?, 'e', ?)", false, `[{"a":"d","b":"e","c":"f"}]`, []interface{}{"d", "f"}},
		{"Values / Named Params", "INSERT INTO test (a, b, c) VALUES ($d, 'e', $f)", false, `[{"a":"d","b":"e","c":"f"}]`, []interface{}{sql.Named("f", "f"), sql.Named("d", "d")}},
		{"Values / Invalid params", "INSERT INTO test (a, b, c) VALUES ('d', ?)", true, "", []interface{}{'e'}},
		{"Select / same table", "INSERT INTO test SELECT * FROM test", true, ``, nil},
	}

	for _, test := range tests {
		testFn := func(withIndexes bool) func(t *testing.T) {
			return func(t *testing.T) {
				db, err := chai.Open(":memory:")
				require.NoError(t, err)
				defer db.Close()

				conn, err := db.Connect()
				require.NoError(t, err)
				defer conn.Close()

				err = conn.Exec("CREATE TABLE test(a TEXT, b TEXT, c TEXT)")
				require.NoError(t, err)
				if withIndexes {
					err = conn.Exec(`
						CREATE INDEX idx_a ON test (a);
						CREATE INDEX idx_b ON test (b);
						CREATE INDEX idx_c ON test (c);
					`)
					require.NoError(t, err)
				}

				err = conn.Exec(test.query, test.params...)
				if test.fails {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)

				st, err := conn.Query("SELECT * FROM test")
				require.NoError(t, err)
				defer st.Close()

				var buf bytes.Buffer
				err = st.MarshalJSONTo(&buf)
				require.NoError(t, err)
				require.JSONEq(t, test.expected, buf.String())
			}
		}

		t.Run("No Index/"+test.name, testFn(false))
		t.Run("With Index/"+test.name, testFn(true))
	}

	t.Run("with RETURNING", func(t *testing.T) {
		db, err := chai.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		err = db.Exec(`CREATE TABLE test(a INT)`)
		require.NoError(t, err)

		d, err := db.QueryRow(`insert into test (a) VALUES (1) RETURNING *, a AS A`)
		require.NoError(t, err)
		testutil.RequireJSONEq(t, d, `{"a": 1,  "A": 1}`)
	})

	t.Run("ensure rollback", func(t *testing.T) {
		db, err := chai.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		conn, err := db.Connect()
		require.NoError(t, err)
		defer conn.Close()

		err = conn.Exec(`CREATE TABLE test(a int unique)`)
		require.NoError(t, err)

		err = conn.Exec(`insert into test (a) VALUES (1), (1)`)
		require.Error(t, err)

		res, err := conn.Query("SELECT * FROM test")
		require.NoError(t, err)
		defer res.Close()

		testutil.RequireStreamEq(t, ``, res)
	})

	t.Run("ON CONFLICT (PK)", func(t *testing.T) {
		db, err := chai.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		err = db.Exec(`CREATE TABLE test(a INT PRIMARY KEY, b INT)`)
		require.NoError(t, err)

		err = db.Exec(`insert into test (a, b) VALUES (1, 1)`)
		require.NoError(t, err)

		err = db.Exec(`insert into test (a, b) VALUES (1, 2) ON CONFLICT DO REPLACE`)
		require.NoError(t, err)

		r, err := db.QueryRow(`SELECT * FROM test`)
		require.NoError(t, err)
		testutil.RequireJSONEq(t, r, `{"a": 1, "b": 2}`)
	})

	t.Run("ON CONFLICT (UNIQUE)", func(t *testing.T) {
		db, err := chai.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		err = db.Exec(`CREATE TABLE test(a INT UNIQUE, b INT)`)
		require.NoError(t, err)

		err = db.Exec(`insert into test (a, b) VALUES (1, 1)`)
		require.NoError(t, err)

		err = db.Exec(`insert into test (a, b) VALUES (1, 2) ON CONFLICT DO REPLACE`)
		require.NoError(t, err)

		r, err := db.QueryRow(`SELECT * FROM test`)
		require.NoError(t, err)
		testutil.RequireJSONEq(t, r, `{"a": 1, "b": 2}`)
	})

	t.Run("SELECT", func(t *testing.T) {
		db, err := chai.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		err = db.Exec(`CREATE TABLE test (a int primary key, b int);
INSERT INTO test (a, b) VALUES (1, 10); UPDATE test SET a = 2, b = 20 WHERE a = 1;INSERT INTO test (a, b) VALUES (1, 10);`)
		require.NoError(t, err)

		conn, err := db.Connect()
		require.NoError(t, err)
		defer conn.Close()

		res, err := conn.Query(`SELECT * FROM test;`)
		require.NoError(t, err)

		var b bytes.Buffer
		err = res.MarshalJSONTo(&b)
		require.NoError(t, err)

		require.JSONEq(t, `
		[
		{"a": 1, "b": 10},
		{"a": 2, "b": 20}
		]
		`, b.String())
	})

	t.Run("with NEXT VALUE FOR", func(t *testing.T) {
		db, err := chai.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		conn, err := db.Connect()
		require.NoError(t, err)
		defer conn.Close()

		err = conn.Exec(`CREATE SEQUENCE seq; CREATE TABLE test(a int, b int default NEXT VALUE FOR seq)`)
		require.NoError(t, err)

		err = conn.Exec(`insert into test (a) VALUES (1), (2), (3)`)
		require.NoError(t, err)

		res, err := conn.Query("SELECT * FROM test")
		require.NoError(t, err)
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
		params   []any
	}{
		{"Same table", `INSERT INTO foo SELECT * FROM foo`, true, ``, nil},
		{"No columns / No projection", `INSERT INTO foo SELECT * FROM bar`, false, `[{"a":1, "b":10, "c":null, "d":null, "e":null}]`, nil},
		{"No columns / Projection", `INSERT INTO foo SELECT a FROM bar`, false, `[{"a":1, "b":null, "c":null, "d":null, "e":null}]`, nil},
		{"With columns / No Projection", `INSERT INTO foo (a, b) SELECT * FROM bar`, true, ``, nil},
		{"With columns / Projection", `INSERT INTO foo (c, d) SELECT a, b FROM bar`, false, `[{"a":null, "b":null, "c":1, "d":10, "e":null}]`, nil},
		{"Too many columns / No Projection", `INSERT INTO foo (c) SELECT * FROM bar`, true, ``, nil},
		{"Too many columns / Projection", `INSERT INTO foo (c, d) SELECT a, b, c FROM bar`, true, ``, nil},
		{"Too few columns / No Projection", `INSERT INTO foo (c, d, e) SELECT * FROM bar`, true, ``, nil},
		{"Too few columns / Projection", `INSERT INTO foo (c, d) SELECT a FROM bar`, true, ``, nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := chai.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			conn, err := db.Connect()
			require.NoError(t, err)
			defer conn.Close()

			err = conn.Exec(`
				CREATE TABLE foo(a INT, b INT, c INT, d INT, e INT);
				CREATE TABLE bar(a INT, b INT, c INT, d INT, e INT);
				INSERT INTO bar (a, b) VALUES (1, 10)
			`)
			require.NoError(t, err)

			err = conn.Exec(test.query, test.params...)
			if test.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			st, err := conn.Query("SELECT * FROM foo")
			require.NoError(t, err)
			defer st.Close()

			var buf bytes.Buffer
			err = st.MarshalJSONTo(&buf)
			require.NoError(t, err)
			require.JSONEq(t, test.expected, buf.String())
		})
	}
}
