package statement_test

import (
	"database/sql"
	"testing"

	"github.com/chaisql/chai/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestInsertStmt(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		fails    bool
		expected string
		params   []any
	}{
		{"Values / Positional Params", "INSERT INTO test (a, b, c) VALUES ($1, 'e', $2)", false, `[{"a":"d","b":"e","c":"f"}]`, []interface{}{"d", "f"}},
		{"Values / Invalid params", "INSERT INTO test (a, b, c) VALUES ('d', $1)", true, "", []any{'e'}},
		{"Select / same table", "INSERT INTO test SELECT * FROM test", true, ``, nil},
	}

	for _, test := range tests {
		testFn := func(withIndexes bool) func(t *testing.T) {
			return func(t *testing.T) {
				db, err := sql.Open("chai", ":memory:")
				require.NoError(t, err)
				defer db.Close()

				_, err = db.Exec("CREATE TABLE test(a TEXT, b TEXT, c TEXT)")
				require.NoError(t, err)
				if withIndexes {
					_, err = db.Exec(`
						CREATE INDEX idx_a ON test (a);
						CREATE INDEX idx_b ON test (b);
						CREATE INDEX idx_c ON test (c);
					`)
					require.NoError(t, err)
				}

				_, err = db.Exec(test.query, test.params...)
				if test.fails {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)

				rows, err := db.Query("SELECT * FROM test")
				require.NoError(t, err)

				testutil.RequireJSONArrayEq(t, rows, test.expected)
			}
		}

		t.Run("No Index/"+test.name, testFn(false))
		t.Run("With Index/"+test.name, testFn(true))
	}

	t.Run("with RETURNING", func(t *testing.T) {
		db, err := sql.Open("chai", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec(`CREATE TABLE test(a INT)`)
		require.NoError(t, err)

		var a, A int
		err = db.QueryRow(`insert into test (a) VALUES (1) RETURNING *, a AS A`).Scan(&a, &A)
		require.NoError(t, err)
		require.Equal(t, 1, a)
		require.Equal(t, 1, A)
	})

	t.Run("ensure rollback", func(t *testing.T) {
		db, err := sql.Open("chai", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec(`CREATE TABLE test(a int unique)`)
		require.NoError(t, err)

		_, err = db.Exec(`insert into test (a) VALUES (1), (1)`)
		require.Error(t, err)

		rows, err := db.Query("SELECT * FROM test")
		require.NoError(t, err)
		defer rows.Close()

		testutil.RequireRowsEq(t, ``, rows)
	})

	t.Run("ON CONFLICT (PK)", func(t *testing.T) {
		db, err := sql.Open("chai", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec(`CREATE TABLE test(a INT PRIMARY KEY, b INT)`)
		require.NoError(t, err)

		_, err = db.Exec(`insert into test (a, b) VALUES (1, 1)`)
		require.NoError(t, err)

		_, err = db.Exec(`insert into test (a, b) VALUES (1, 2) ON CONFLICT DO REPLACE`)
		require.NoError(t, err)

		var a, b int
		err = db.QueryRow(`SELECT * FROM test`).Scan(&a, &b)
		require.NoError(t, err)
		require.Equal(t, 1, a)
		require.Equal(t, 2, b)
	})

	t.Run("ON CONFLICT (UNIQUE)", func(t *testing.T) {
		db, err := sql.Open("chai", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec(`CREATE TABLE test(a INT UNIQUE, b INT)`)
		require.NoError(t, err)

		_, err = db.Exec(`insert into test (a, b) VALUES (1, 1)`)
		require.NoError(t, err)

		_, err = db.Exec(`insert into test (a, b) VALUES (1, 2) ON CONFLICT DO REPLACE`)
		require.NoError(t, err)

		var a, b int
		err = db.QueryRow(`SELECT * FROM test`).Scan(&a, &b)
		require.NoError(t, err)
		require.Equal(t, 1, a)
		require.Equal(t, 2, b)
	})

	t.Run("SELECT", func(t *testing.T) {
		db, err := sql.Open("chai", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec(`CREATE TABLE test (a int primary key, b int);
INSERT INTO test (a, b) VALUES (1, 10); UPDATE test SET a = 2, b = 20 WHERE a = 1;INSERT INTO test (a, b) VALUES (1, 10);`)
		require.NoError(t, err)

		rows, err := db.Query(`SELECT * FROM test;`)
		require.NoError(t, err)
		testutil.RequireJSONArrayEq(t, rows, `
		[
		{"a": 1, "b": 10},
		{"a": 2, "b": 20}
		]
		`)
	})

	t.Run("with NEXT VALUE FOR", func(t *testing.T) {
		db, err := sql.Open("chai", ":memory:")
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Exec(`CREATE SEQUENCE seq; CREATE TABLE test(a int, b int default NEXT VALUE FOR seq)`)
		require.NoError(t, err)

		_, err = db.Exec(`insert into test (a) VALUES (1), (2), (3)`)
		require.NoError(t, err)

		res, err := db.Query("SELECT * FROM test")
		require.NoError(t, err)

		testutil.RequireJSONArrayEq(t, res, `
		[
			{"a": 1, "b": 1},
			{"a": 2, "b": 2},
			{"a": 3, "b": 3}
		]
		`)
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
			db, err := sql.Open("chai", ":memory:")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec(`
				CREATE TABLE foo(a INT, b INT, c INT, d INT, e INT);
				CREATE TABLE bar(a INT, b INT, c INT, d INT, e INT);
				INSERT INTO bar (a, b) VALUES (1, 10)
			`)
			require.NoError(t, err)

			_, err = db.Exec(test.query, test.params...)
			if test.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			rows, err := db.Query("SELECT * FROM foo")
			require.NoError(t, err)
			defer rows.Close()

			testutil.RequireJSONArrayEq(t, rows, test.expected)
		})
	}
}
