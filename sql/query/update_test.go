package query_test

import (
	"bytes"
	"database/sql"
	"testing"

	"github.com/asdine/genji"
	"github.com/asdine/genji/document"
	"github.com/stretchr/testify/require"
)

func TestUpdateStmt(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		fails    bool
		expected string
		params   []interface{}
	}{
		{"No cond", `UPDATE test SET a = 'boo'`, false, `[{"a":"boo","b":"bar1","c":"baz1"},{"a":"boo","b":"bar2"},{"d":"foo3","e":"bar3"}]`, nil},
		{"No cond / with ident string", "UPDATE test SET `a` = 'boo'", false, `[{"a":"boo","b":"bar1","c":"baz1"},{"a":"boo","b":"bar2"},{"d":"foo3","e":"bar3"}]`, nil},
		{"No cond / with multiple idents", `UPDATE test SET a = c`, false, `[{"a":"baz1","b":"bar1","c":"baz1"},{"a":null,"b":"bar2"},{"d":"foo3","e":"bar3"}]`, nil},
		{"No cond / with string", `UPDATE test SET 'a' = 'boo'`, true, "", nil},
		{"With cond", "UPDATE test SET a = 1, b = 2 WHERE a = 'foo2'", false, `[{"a":"foo1","b":"bar1","c":"baz1"},{"a":1,"b":2},{"d":"foo3","e":"bar3"}]`, nil},
		{"Field not found", "UPDATE test SET a = 1, b = 2 WHERE a = f", false, `[{"a":"foo1","b":"bar1","c":"baz1"},{"a":"foo2","b":"bar2"},{"d":"foo3","e":"bar3"}]`, nil},
		{"Positional params", "UPDATE test SET a = ?, b = ? WHERE a = ?", false, `[{"a":"a","b":"b","c":"baz1"},{"a":"foo2","b":"bar2"},{"d":"foo3","e":"bar3"}]`, []interface{}{"a", "b", "foo1"}},
		{"Named params", "UPDATE test SET a = $a, b = $b WHERE a = $c", false, `[{"a":"a","b":"b","c":"baz1"},{"a":"foo2","b":"bar2"},{"d":"foo3","e":"bar3"}]`, []interface{}{sql.Named("b", "b"), sql.Named("a", "a"), sql.Named("c", "foo1")}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec("CREATE TABLE test")
			require.NoError(t, err)
			err = db.Exec("INSERT INTO test (a, b, c) VALUES ('foo1', 'bar1', 'baz1')")
			require.NoError(t, err)
			err = db.Exec("INSERT INTO test (a, b) VALUES ('foo2', 'bar2')")
			require.NoError(t, err)
			err = db.Exec("INSERT INTO test (d, e) VALUES ('foo3', 'bar3')")
			require.NoError(t, err)

			err = db.Exec(test.query, test.params...)
			if test.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			st, err := db.Query("SELECT * FROM test")
			require.NoError(t, err)
			defer st.Close()

			var buf bytes.Buffer

			err = document.IteratorToJSONArray(&buf, st)
			require.NoError(t, err)
			require.JSONEq(t, test.expected, buf.String())
		})
	}
}
