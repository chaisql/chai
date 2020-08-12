package query_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
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
		{"Values / No columns", `INSERT INTO test VALUES ("a", 'b', 'c')`, true, ``, nil},
		{"Values / With columns", `INSERT INTO test (a, b, c) VALUES ('a', 'b', 'c')`, false, `{"pk()":1,"a":"a","b":"b","c":"c"}`, nil},
		{"Values / Ident", `INSERT INTO test (a) VALUES (a)`, true, ``, nil},
		{"Values / Ident string", "INSERT INTO test (a) VALUES (`a`)", true, ``, nil},
		{"Values / With fields ident string", "INSERT INTO test (a, `foo bar`) VALUES ('c', 'd')", false, `{"pk()":1,"a":"c","foo bar":"d"}`, nil},
		{"Values / Positional Params", "INSERT INTO test (a, b, c) VALUES (?, 'e', ?)", false, `{"pk()":1,"a":"d","b":"e","c":"f"}`, []interface{}{"d", "f"}},
		{"Values / Named Params", "INSERT INTO test (a, b, c) VALUES ($d, 'e', $f)", false, `{"pk()":1,"a":"d","b":"e","c":"f"}`, []interface{}{sql.Named("f", "f"), sql.Named("d", "d")}},
		{"Values / Invalid params", "INSERT INTO test (a, b, c) VALUES ('d', ?)", true, "", []interface{}{'e'}},
		{"Values / List", `INSERT INTO test (a, b, c) VALUES ("a", 'b', [1, 2, 3])`, false, `{"pk()":1,"a":"a","b":"b","c":[1,2,3]}`, nil},
		{"Values / Document", `INSERT INTO test (a, b, c) VALUES ("a", 'b', {c: 1, d: c + 1})`, false, `{"pk()":1,"a":"a","b":"b","c":{"c":1,"d":2}}`, nil},
		{"Documents", "INSERT INTO test VALUES {a: 'a', b: 2.3, c: 1 = 1}", false, `{"pk()":1,"a":"a","b":2.3,"c":true}`, nil},
		{"Documents / Positional Params", "INSERT INTO test VALUES {a: ?, b: 2.3, c: ?}", false, `{"pk()":1,"a":"a","b":2.3,"c":true}`, []interface{}{"a", true}},
		{"Documents / Named Params", "INSERT INTO test VALUES {a: $a, b: 2.3, c: $c}", false, `{"pk()":1,"a":1,"b":2.3,"c":true}`, []interface{}{sql.Named("c", true), sql.Named("a", 1)}},
		{"Documents / List ", "INSERT INTO test VALUES {a: [1, 2, 3]}", false, `{"pk()":1,"a":[1,2,3]}`, nil},
		{"Documents / strings", `INSERT INTO test VALUES {'a': 'a', b: 2.3}`, false, `{"pk()":1,"a":"a","b":2.3}`, nil},
		{"Documents / double quotes", `INSERT INTO test VALUES {"a": "b"}`, false, `{"pk()":1,"a":"b"}`, nil},
		{"Documents / with reference to other fields", `INSERT INTO test VALUES {a: 400, b: a * 4}`, false, `{"pk()":1,"a":400,"b":1600}`, nil},
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
				err = document.IteratorToJSON(&buf, st)
				require.NoError(t, err)
				require.JSONEq(t, test.expected, buf.String())
			}
		}

		t.Run("No Index/"+test.name, testFn(false))
		t.Run("With Index/"+test.name, testFn(true))
	}

	t.Run("with primary key", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		err = db.Exec("CREATE TABLE test (foo INTEGER PRIMARY KEY)")
		require.NoError(t, err)

		err = db.Exec(`INSERT INTO test (bar) VALUES (1)`)
		require.Error(t, err)
		err = db.Exec(`INSERT INTO test (bar, foo) VALUES (1, 2)`)
		require.NoError(t, err)

		err = db.Exec(`INSERT INTO test (bar, foo) VALUES (1, 2)`)
		require.Equal(t, err, database.ErrDuplicateDocument)
	})

	t.Run("with shadowing", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		err = db.Exec("CREATE TABLE test")
		require.NoError(t, err)

		err = db.Exec("INSERT INTO test (`pk()`, `key`) VALUES (1, 2)")
		require.NoError(t, err)
	})

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
		err = document.IteratorToJSON(&buf, res)
		require.NoError(t, err)
		require.JSONEq(t, `{"a": "a", "b-b": "b"}`, buf.String())
	})

	t.Run("with types constraints", func(t *testing.T) {
		// This test ensures that we can insert data into every supported types.
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		err = db.Exec(`CREATE TABLE test(
			b bool, db double,
			i integer, du duration, bb blob, byt bytes,
			t text, a array, d document
		)`)
		require.NoError(t, err)

		err = db.Exec(`
			INSERT INTO test
			VALUES {
				i: 10000000000, db: 21.21, b: true,
				du: 127ns, bb: "YmxvYlZhbHVlCg==", byt: "Ynl0ZXNWYWx1ZQ==",
				t: "text", a: [1, "foo", true], d: {"foo": "bar"}
			}`)
		require.NoError(t, err)

		res, err := db.Query("SELECT * FROM test")
		defer res.Close()
		require.NoError(t, err)

		var buf bytes.Buffer
		err = document.IteratorToJSON(&buf, res)
		require.NoError(t, err)
		require.JSONEq(t, `{
			"i": 10000000000,
			"db": 21.21,
			"b": true,
			"du": "127ns",
			"bb": "YmxvYlZhbHVlCg==",
			"byt": "Ynl0ZXNWYWx1ZQ==",
			"t": "text",
			"a": [1, "foo", true],
			"d": {"foo": "bar"}
		  }`, buf.String())
	})

	t.Run("with tests that require an error", func(t *testing.T) {
		tests := []struct {
			name            string
			fieldConstraint string
			value           string
		}{
			{"not null without type constraint", "NOT NULL", `{}`},

			{"array / not null with type constraint", "ARRAY NOT NULL", `{}`},
			{"array / not null with non-respected type constraint ", "ARRAY NOT NULL", `{a: 42}`},

			{"blob", "BLOB", `{a: true}`},
			{"blob / not null with type constraint", "BLOB NOT NULL", `{}`},
			{"blob / not null with non-respected type constraint ", "BLOB NOT NULL", `{a: 42}`},

			{"bool / not null with type constraint", "BOOL NOT NULL", `{}`},

			{"bytes", "BYTES", `{a: [1,2,3]}`},
			{"bytes / not null with type constraint", "BYTES NOT NULL", `{}`},
			{"bytes / not null with non-respected type constraint ", "BYTES NOT NULL", `{a: 42}`},

			{"document", "DOCUMENT", `{"a": "foo"}`},
			{"document / not null with type constraint", "DOCUMENT NOT NULL", `{}`},
			{"document / not null with non-respected type constraint ", "DOCUMENT NOT NULL", `{a: false}`},

			{"duration", "DURATION", `{a: "foo"}`},
			{"duration / not null with type constraint", "DURATION NOT NULL", `{}`},
			{"duration / not null with non-respected type constraint ", "DURATION NOT NULL", `{a: [1,2,3]}`},

			{"double", "DOUBLE", `{a: "foo"}`},
			{"double / not null with type constraint", "DOUBLE NOT NULL", `{}`},
			{"double / not null with non-respected type constraint ", "DOUBLE NOT NULL", `{a: [1,2,3]}`},

			{"integer", "INTEGER", `{a: "foo"}`},
			{"integer / not null with type constraint", "INTEGER NOT NULL", `{}`},
			{"integer / not null with non-respected type constraint ", "INTEGER NOT NULL", `{a: [1,2,3]}`},

			{"text / not null with type constraint", "TEXT NOT NULL", `{}`},
		}

		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		for i, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				q := fmt.Sprintf("CREATE TABLE test%d (a %s)", i, test.fieldConstraint)
				err := db.Exec(q)
				require.NoError(t, err)

				q = fmt.Sprintf("INSERT INTO test%d VALUES %s", i, test.value)
				err = db.Exec(q)
				require.Error(t, err)
			})
		}
	})
}
