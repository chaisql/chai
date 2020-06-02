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
		{"Values / List", `INSERT INTO test (a, b, c) VALUES ("a", 'b', (1, 2, 3))`, false, `{"pk()":1,"a":"a","b":"b","c":[1,2,3]}`, nil},
		{"Documents", "INSERT INTO test VALUES {a: 'a', b: 2.3, c: 1 = 1}", false, `{"pk()":1,"a":"a","b":2.3,"c":true}`, nil},
		{"Documents / Positional Params", "INSERT INTO test VALUES {a: ?, b: 2.3, c: ?}", false, `{"pk()":1,"a":"a","b":2.3,"c":true}`, []interface{}{"a", true}},
		{"Documents / Named Params", "INSERT INTO test VALUES {a: $a, b: 2.3, c: $c}", false, `{"pk()":1,"a":1,"b":2.3,"c":true}`, []interface{}{sql.Named("c", true), sql.Named("a", 1)}},
		{"Documents / List ", "INSERT INTO test VALUES {a: (1, 2, 3)}", false, `{"pk()":1,"a":[1,2,3]}`, nil},
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
			i8 int8, i16 int16, i32 int32, i64 int64, f64 float64, b bool,
			i int, ig integer, n numeric, du duration, bb blob, byt bytes,
			t text, s string, a array, d document
		)`)
		require.NoError(t, err)

		err = db.Exec(`
			INSERT INTO test
			VALUES {
				i8: 100, i16: 1000, i32: 10000,	i64: 10000000000, f64: 21.21, b: true,
				i: 1000, ig: 1000, n: 21.21, du: 127ns, bb: "blobValue", byt: "bytesValue",
				t: "text", s: "string", a: [1, "foo", true], d: {"foo": "bar"}
			}`)
		require.NoError(t, err)

		res, err := db.Query("SELECT * FROM test")
		defer res.Close()
		require.NoError(t, err)

		var buf bytes.Buffer
		err = document.IteratorToJSON(&buf, res)
		require.NoError(t, err)
		require.JSONEq(t, `{
			"i8": 100,
			"i16": 1000,
			"i32": 10000,
			"i64": 10000000000,
			"f64": 21.21,
			"b": true,
			"i": 1000,
			"ig": 1000,
			"n": 21.21,
			"du": 127,
			"bb": "blobValue",
			"byt": "bytesValue",
			"t": "text",
			"s": "string",
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

			{"array", "ARRAY", `{a: "[1,2,3]"}`, `cannot convert "text" to "array"`},
			{"array / not null with type constraint", "ARRAY NOT NULL", `{}`, `field "a" is required and must be not null`},
			{"array / not null with non-respected type constraint ", "ARRAY NOT NULL", `{a: 42}`, `cannot convert "int8" to "array"`},

			{"blob", "BLOB", `{a: true}`, `cannot convert "bool" to "bytes"`},
			{"blob / not null with type constraint", "BLOB NOT NULL", `{}`, `field "a" is required and must be not null`},
			{"blob / not null with non-respected type constraint ", "BLOB NOT NULL", `{a: 42}`, `cannot convert "int8" to "bytes"`},

			{"bool / not null with type constraint", "BOOL NOT NULL", `{}`, `field "a" is required and must be not null`},

			{"bytes", "BYTES", `{a: [1,2,3]}`, `cannot convert "array" to "bytes"`},
			{"bytes / not null with type constraint", "BYTES NOT NULL", `{}`, `field "a" is required and must be not null`},
			{"bytes / not null with non-respected type constraint ", "BYTES NOT NULL", `{a: 42}`, `cannot convert "int8" to "bytes"`},

			{"document", "DOCUMENT", `{a: "foo"}`, `cannot convert "text" to "document"`},
			{"document / not null with type constraint", "DOCUMENT NOT NULL", `{}`, `field "a" is required and must be not null`},
			{"document / not null with non-respected type constraint ", "DOCUMENT NOT NULL", `{a: false}`, `cannot convert "bool" to "document"`},

			{"duration", "DURATION", `{a: "foo"}`, `cannot convert "foo" to "duration": time: invalid duration foo`},
			{"duration / not null with type constraint", "DURATION NOT NULL", `{}`, `field "a" is required and must be not null`},
			{"duration / not null with non-respected type constraint ", "DURATION NOT NULL", `{a: [1,2,3]}`, `type "array" incompatible with "integer"`},

			{"float64", "FLOAT64", `{a: "foo"}`, `cannot convert "text" to "float64"`},
			{"float64 / not null with type constraint", "FLOAT64 NOT NULL", `{}`, `field "a" is required and must be not null`},
			{"float64 / not null with non-respected type constraint ", "FLOAT64 NOT NULL", `{a: [1,2,3]}`, `cannot convert "array" to "float64"`},

			{"int", "INT", `{a: "foo"}`, `cannot convert "text" to "int64": type "text" incompatible with "integer"`},
			{"int / not null with type constraint", "INT NOT NULL", `{}`, `field "a" is required and must be not null`},
			{"int / not null with non-respected type constraint ", "INT NOT NULL", `{a: [1,2,3]}`, `cannot convert "array" to "int64": type "array" incompatible with "integer"`},

			{"integer", "INTEGER", `{a: "foo"}`, `cannot convert "text" to "int64": type "text" incompatible with "integer"`},
			{"integer / not null with type constraint", "INTEGER NOT NULL", `{}`, `field "a" is required and must be not null`},
			{"integer / not null with non-respected type constraint ", "INTEGER NOT NULL", `{a: [1,2,3]}`, `cannot convert "array" to "int64": type "array" incompatible with "integer"`},

			{"int8", "INT8", `{a: "foo"}`, `cannot convert "text" to "int8": type "text" incompatible with "integer"`},
			{"int8 / not null with type constraint", "INT8 NOT NULL", `{}`, `field "a" is required and must be not null`},
			{"int8 / not null with non-respected type constraint ", "INT8 NOT NULL", `{a: [1,2,3]}`, `cannot convert "array" to "int8": type "array" incompatible with "integer"`},

			{"int16", "INT16", `{a: "foo"}`, `cannot convert "text" to "int16": type "text" incompatible with "integer"`},
			{"int16 / not null with type constraint", "INT16 NOT NULL", `{}`, `field "a" is required and must be not null`},
			{"int16 / not null with non-respected type constraint ", "INT16 NOT NULL", `{a: [1,2,3]}`, `cannot convert "array" to "int16": type "array" incompatible with "integer"`},

			{"int32", "INT32", `{a: "foo"}`, `cannot convert "text" to "int32": type "text" incompatible with "integer"`},
			{"int32 / not null with type constraint", "INT32 NOT NULL", `{}`, `field "a" is required and must be not null`},
			{"int32 / not null with non-respected type constraint ", "INT32 NOT NULL", `{a: [1,2,3]}`, `cannot convert "array" to "int32": type "array" incompatible with "integer"`},

			{"int64", "INT64", `{a: "foo"}`, `cannot convert "text" to "int64": type "text" incompatible with "integer"`},
			{"int64 / not null with type constraint", "INT64 NOT NULL", `{}`, `field "a" is required and must be not null`},
			{"int64 / not null with non-respected type constraint ", "INT64 NOT NULL", `{a: [1,2,3]}`, `cannot convert "array" to "int64": type "array" incompatible with "integer"`},

			{"numeric", "NUMERIC", `{a: "foo"}`, `cannot convert "text" to "float64"`},
			{"numeric / not null with type constraint", "NUMERIC NOT NULL", `{}`, `field "a" is required and must be not null`},
			{"numeric / not null with non-respected type constraint ", "NUMERIC NOT NULL", `{a: [1,2,3]}`, `cannot convert "array" to "float64"`},

			{"string", "STRING", `{a: [1,2,3]}`, `cannot convert "array" to "string"`},
			{"string / not null with type constraint", "STRING NOT NULL", `{}`, `field "a" is required and must be not null`},
			{"string / not null with non-respected type constraint ", "STRING NOT NULL", `{a: 420}`, `cannot convert "int16" to "string"`},

			{"text", "TEXT", `{a: [1,2,3]}`, `cannot convert "array" to "string"`},
			{"text / not null with type constraint", "TEXT NOT NULL", `{}`, `field "a" is required and must be not null`},
			{"text / not null with non-respected type constraint ", "TEXT NOT NULL", `{a: 42}`, `cannot convert "int8" to "string"`},
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
