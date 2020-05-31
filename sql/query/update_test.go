package query_test

import (
	"bytes"
	"database/sql"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
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
		// Test without any clause.
		{"No clause", `UPDATE test`, true, "", nil},

		// SET tests.
		{"SET / No cond", `UPDATE test SET a = 'boo'`, false, `[{"a":"boo","b":"bar1","c":"baz1"},{"a":"boo","b":"bar2"},{"a":"boo","d":"bar3","e":"baz3"}]`, nil},
		{"SET / No cond / with ident string", "UPDATE test SET `a` = 'boo'", false, `[{"a":"boo","b":"bar1","c":"baz1"},{"a":"boo","b":"bar2"},{"a":"boo","d":"bar3","e":"baz3"}]`, nil},
		{"SET / No cond / with multiple idents and constraint", `UPDATE test SET a = c`, true, ``, nil},
		{"SET / No cond / with multiple idents", `UPDATE test SET b = c`, false, `[{"a":"foo1","b":"baz1","c":"baz1"},{"a":"foo2","b":null},{"a":"foo3","b":null,"d":"bar3","e":"baz3"}]`, nil},
		{"SET / No cond / with missing field", "UPDATE test SET f = 'boo'", false, `[{"a":"foo1","b":"bar1","c":"baz1","f":"boo"},{"a":"foo2","b":"bar2","f":"boo"},{"a":"foo3","d":"bar3","e":"baz3","f":"boo"}]`, nil},
		{"SET / No cond / with string", `UPDATE test SET 'a' = 'boo'`, true, "", nil},
		{"SET / With cond", "UPDATE test SET a = 'FOO2', b = 2 WHERE a = 'foo2'", false, `[{"a":"foo1","b":"bar1","c":"baz1"},{"a":"FOO2","b":2},{"a":"foo3","d":"bar3","e":"baz3"}]`, nil},
		{"SET / With cond / with missing field", "UPDATE test SET f = 'boo' WHERE d = 'bar3'", false, `[{"a":"foo1","b":"bar1","c":"baz1"},{"a":"foo2","b":"bar2"},{"a":"foo3","d":"bar3","e":"baz3","f":"boo"}]`, nil},
		{"SET / Field not found", "UPDATE test SET a = 1, b = 2 WHERE a = f", false, `[{"a":"foo1","b":"bar1","c":"baz1"},{"a":"foo2","b":"bar2"},{"a":"foo3","d":"bar3","e":"baz3"}]`, nil},
		{"SET / Positional params", "UPDATE test SET a = ?, b = ? WHERE a = ?", false, `[{"a":"a","b":"b","c":"baz1"},{"a":"foo2","b":"bar2"},{"a":"foo3","d":"bar3","e":"baz3"}]`, []interface{}{"a", "b", "foo1"}},
		{"SET / Named params", "UPDATE test SET a = $a, b = $b WHERE a = $c", false, `[{"a":"a","b":"b","c":"baz1"},{"a":"foo2","b":"bar2"},{"a":"foo3","d":"bar3","e":"baz3"}]`, []interface{}{sql.Named("b", "b"), sql.Named("a", "a"), sql.Named("c", "foo1")}},

		// UNSET tests.
		{"UNSET / No cond", `UPDATE test UNSET b`, false, `[{"a":"foo1","c":"baz1"},{"a":"foo2"},{"a":"foo3","d":"bar3","e":"baz3"}]`, nil},
		{"UNSET / No cond / with ident string", "UPDATE test UNSET `a`", true, "", nil},
		{"UNSET / No cond / with missing field", "UPDATE test UNSET f", false, `[{"a":"foo1","b":"bar1","c":"baz1"},{"a":"foo2","b":"bar2"},{"a":"foo3","d":"bar3","e":"baz3"}]`, nil},
		{"UNSET / No cond / with string", `UPDATE test UNSET 'a'`, true, "", nil},
		{"UNSET / With cond", `UPDATE test UNSET b WHERE a = 'foo2'`, false, `[{"a":"foo1","b":"bar1","c":"baz1"},{"a":"foo2"},{"a":"foo3","d":"bar3","e":"baz3"}]`, nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec("CREATE TABLE test (a text not null)")
			require.NoError(t, err)
			err = db.Exec("INSERT INTO test (a, b, c) VALUES ('foo1', 'bar1', 'baz1')")
			require.NoError(t, err)
			err = db.Exec("INSERT INTO test (a, b) VALUES ('foo2', 'bar2')")
			require.NoError(t, err)
			err = db.Exec("INSERT INTO test (a, d, e) VALUES ('foo3', 'bar3', 'baz3')")
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
