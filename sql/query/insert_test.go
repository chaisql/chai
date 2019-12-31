package query_test

import (
	"bytes"
	"database/sql"
	"testing"

	"github.com/asdine/genji"
	"github.com/asdine/genji/database"
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/engine/memoryengine"
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
		{"Values / With columns", `INSERT INTO test (a, b, c) VALUES ('a', 'b', 'c')`, false, `{"key()":1,"a":"a","b":"b","c":"c"}`, nil},
		{"Values / Ident", `INSERT INTO test (a) VALUES (a)`, true, ``, nil},
		{"Values / Ident string", "INSERT INTO test (a) VALUES (`a`)", true, ``, nil},
		{"Values / With fields ident string", "INSERT INTO test (a, `foo bar`) VALUES ('c', 'd')", false, `{"key()":1,"a":"c","foo bar":"d"}`, nil},
		{"Values / Positional Params", "INSERT INTO test (a, b, c) VALUES (?, 'e', ?)", false, `{"key()":1,"a":"d","b":"e","c":"f"}`, []interface{}{"d", "f"}},
		{"Values / Named Params", "INSERT INTO test (a, b, c) VALUES ($d, 'e', $f)", false, `{"key()":1,"a":"d","b":"e","c":"f"}`, []interface{}{sql.Named("f", "f"), sql.Named("d", "d")}},
		{"Values / Invalid params", "INSERT INTO test (a, b, c) VALUES ('d', ?)", true, "", []interface{}{'e'}},
		{"Values / List", `INSERT INTO test (a, b, c) VALUES ("a", 'b', (1, 2, 3))`, false, `{"key()":1,"a":"a","b":"b","c":[1,2,3]}`, nil},
		{"Documents", "INSERT INTO test VALUES {a: 'a', b: 2.3, c: 1 = 1}", false, `{"key()":1,"a":"a","b":2.3,"c":true}`, nil},
		{"Documents / Positional Params", "INSERT INTO test VALUES {a: ?, b: 2.3, c: ?}", false, `{"key()":1,"a":"a","b":2.3,"c":true}`, []interface{}{"a", true}},
		{"Documents / Named Params", "INSERT INTO test VALUES {a: $a, b: 2.3, c: $c}", false, `{"key()":1,"a":1,"b":2.3,"c":true}`, []interface{}{sql.Named("c", true), sql.Named("a", 1)}},
		{"Documents / List ", "INSERT INTO test VALUES {a: (1, 2, 3)}", false, `{"key()":1,"a":[1,2,3]}`, nil},
		{"Documents / strings", `INSERT INTO test VALUES {'a': 'a', b: 2.3}`, false, `{"key()":1,"a":"a","b":2.3}`, nil},
		{"Documents / double quotes", `INSERT INTO test VALUES {"a": "b"}`, false, `{"key()":1,"a":"b"}`, nil},
	}

	for _, test := range tests {
		testFn := func(withIndexes bool) func(t *testing.T) {
			return func(t *testing.T) {
				db, err := genji.New(memoryengine.NewEngine())
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

				st, err := db.Query("SELECT key(), * FROM test")
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
		db, err := genji.New(memoryengine.NewEngine())
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
		db, err := genji.New(memoryengine.NewEngine())
		require.NoError(t, err)
		defer db.Close()

		err = db.Exec("CREATE TABLE test")
		require.NoError(t, err)

		err = db.Exec("INSERT INTO test (`key()`, `key`) VALUES (1, 2)")
		require.NoError(t, err)
	})

	t.Run("with struct param", func(t *testing.T) {
		db, err := genji.New(memoryengine.NewEngine())
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
}
