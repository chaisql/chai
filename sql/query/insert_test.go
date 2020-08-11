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
		{"Documents", "INSERT INTO test VALUES {a: 'a', b: 2.3, c: 1 = 1}", false, `{"pk()":1,"a":"a","b":2.3,"c":true}`, nil},
		{"Documents / Positional Params", "INSERT INTO test VALUES {a: ?, b: 2.3, c: ?}", false, `{"pk()":1,"a":"a","b":2.3,"c":true}`, []interface{}{"a", true}},
		{"Documents / Named Params", "INSERT INTO test VALUES {a: $a, b: 2.3, c: $c}", false, `{"pk()":1,"a":1,"b":2.3,"c":true}`, []interface{}{sql.Named("c", true), sql.Named("a", 1)}},
		{"Documents / List ", "INSERT INTO test VALUES {a: [1, 2, 3]}", false, `{"pk()":1,"a":[1,2,3]}`, nil},
		{"Documents / strings", `INSERT INTO test VALUES {'a': 'a', b: 2.3}`, false, `{"pk()":1,"a":"a","b":2.3}`, nil},
		{"Documents / double quotes", `INSERT INTO test VALUES {"a": "b"}`, false, `{"pk()":1,"a":"b"}`, nil},
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
			expectedErr     string
		}{
			{"not null without type constraint", "NOT NULL", `{}`, `field "a" is required and must be not null`},

			{"array / not null with type constraint", "ARRAY NOT NULL", `{}`, `field "a" is required and must be not null`},
			{"array / not null with non-respected type constraint ", "ARRAY NOT NULL", `{a: 42}`, `cannot cast integer as array`},

			{"blob", "BLOB", `{a: true}`, `cannot cast bool as blob`},
			{"blob / not null with type constraint", "BLOB NOT NULL", `{}`, `field "a" is required and must be not null`},
			{"blob / not null with non-respected type constraint ", "BLOB NOT NULL", `{a: 42}`, `cannot cast integer as blob`},

			{"bool / not null with type constraint", "BOOL NOT NULL", `{}`, `field "a" is required and must be not null`},

			{"bytes", "BYTES", `{a: [1,2,3]}`, `cannot cast array as blob`},
			{"bytes / not null with type constraint", "BYTES NOT NULL", `{}`, `field "a" is required and must be not null`},
			{"bytes / not null with non-respected type constraint ", "BYTES NOT NULL", `{a: 42}`, `cannot cast integer as blob`},

			{"document", "DOCUMENT", `{"a": "foo"}`, `cannot cast "foo" as document: found "\x00", expected '{'`},
			{"document / not null with type constraint", "DOCUMENT NOT NULL", `{}`, `field "a" is required and must be not null`},
			{"document / not null with non-respected type constraint ", "DOCUMENT NOT NULL", `{a: false}`, `cannot cast bool as document`},

			{"duration", "DURATION", `{a: "foo"}`, `cannot cast "foo" as duration: time: invalid duration "foo"`},
			{"duration / not null with type constraint", "DURATION NOT NULL", `{}`, `field "a" is required and must be not null`},
			{"duration / not null with non-respected type constraint ", "DURATION NOT NULL", `{a: [1,2,3]}`, `cannot cast array as duration`},

			{"double", "DOUBLE", `{a: "foo"}`, `cannot cast "foo" as double: strconv.ParseFloat: parsing "foo": invalid syntax`},
			{"double / not null with type constraint", "DOUBLE NOT NULL", `{}`, `field "a" is required and must be not null`},
			{"double / not null with non-respected type constraint ", "DOUBLE NOT NULL", `{a: [1,2,3]}`, `cannot cast array as double`},

			{"integer", "INTEGER", `{a: "foo"}`, `cannot cast "foo" as integer: strconv.ParseInt: parsing "foo": invalid syntax`},
			{"integer / not null with type constraint", "INTEGER NOT NULL", `{}`, `field "a" is required and must be not null`},
			{"integer / not null with non-respected type constraint ", "INTEGER NOT NULL", `{a: [1,2,3]}`, `cannot cast array as integer`},

			{"text / not null with type constraint", "TEXT NOT NULL", `{}`, `field "a" is required and must be not null`},
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
				require.EqualError(t, err, test.expectedErr)
			})
		}
	})
}
