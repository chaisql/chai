package statement_test

import (
	"bytes"
	"database/sql"
	"testing"

	"github.com/chaisql/chai"
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
		{"No clause", `UPDATE test`, true, "", nil},
		{"Read-only table", `UPDATE __chai_catalog SET a = 1`, true, "", nil},

		{"SET / No cond", `UPDATE test SET a = 'boo'`, false, `[{"a":"boo","b":"bar1","c":"baz1","d":null,"e":null},{"a":"boo","b":"bar2","c":null,"d":null,"e":null},{"a":"boo","d":"bar3","e":"baz3","c":null,"b":null}]`, nil},
		{"SET / No cond / with ident string", "UPDATE test SET `a` = 'boo'", false, `[{"a":"boo","b":"bar1","c":"baz1","d":null,"e":null},{"a":"boo","b":"bar2","c":null,"d":null,"e":null},{"a":"boo","d":"bar3","e":"baz3","c":null,"b":null}]`, nil},
		{"SET / No cond / with multiple idents and constraint", `UPDATE test SET a = c`, true, ``, nil},
		{"SET / No cond / with multiple idents", `UPDATE test SET b = c`, false, `[{"a":"foo1","b":"baz1","c":"baz1","d":null,"e":null},{"a":"foo2","b":null,"c":null,"d":null,"e":null},{"a":"foo3","b":null,"c":null,"d":"bar3","e":"baz3"}]`, nil},
		{"SET / No cond / with missing column", "UPDATE test SET f = 'boo'", true, "", nil},
		{"SET / No cond / with string", `UPDATE test SET 'a' = 'boo'`, true, "", nil},
		{"SET / With cond", "UPDATE test SET a = 'FOO2', b = 2 WHERE a = 'foo2'", false, `[{"a":"foo1","b":"bar1","c":"baz1","d":null,"e":null},{"a":"FOO2","b":"2","c":null,"d":null,"e":null},{"a":"foo3","b":null,"c":null,"d":"bar3","e":"baz3"}]`, nil},
		{"SET / With cond / with missing column", "UPDATE test SET f = 'boo' WHERE d = 'bar3'", true, ``, nil},
		{"SET / Field not found", "UPDATE test SET a = 1, b = 2 WHERE a = f", true, ``, nil},
		{"SET / Positional params", "UPDATE test SET a = ?, b = ? WHERE a = ?", false, `[{"a":"a","b":"b","c":"baz1","d":null,"e":null},{"a":"foo2","b":"bar2","c":null,"d":null,"e":null},{"a":"foo3","b":null,"c":null,"d":"bar3","e":"baz3"}]`, []interface{}{"a", "b", "foo1"}},
		{"SET / Named params", "UPDATE test SET a = $a, b = $b WHERE a = $c", false, `[{"a":"a","b":"b","c":"baz1","d":null,"e":null},{"a":"foo2","b":"bar2","c":null,"d":null,"e":null},{"a":"foo3","b":null,"c":null,"d":"bar3","e":"baz3"}]`, []interface{}{sql.Named("b", "b"), sql.Named("a", "a"), sql.Named("c", "foo1")}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runTest := func(indexed bool) {
				db, err := chai.Open(":memory:")
				require.NoError(t, err)
				defer db.Close()

				conn, err := db.Connect()
				require.NoError(t, err)
				defer conn.Close()

				err = conn.Exec("CREATE TABLE test (a text not null, b text, c text, d text, e text)")
				require.NoError(t, err)

				if indexed {
					err = conn.Exec("CREATE INDEX idx_test_a ON test(a)")
					require.NoError(t, err)
				}

				err = conn.Exec("INSERT INTO test (a, b, c) VALUES ('foo1', 'bar1', 'baz1')")
				require.NoError(t, err)
				err = conn.Exec("INSERT INTO test (a, b) VALUES ('foo2', 'bar2')")
				require.NoError(t, err)
				err = conn.Exec("INSERT INTO test (a, d, e) VALUES ('foo3', 'bar3', 'baz3')")
				require.NoError(t, err)

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

			// runTest(false)
			runTest(true)
		})
	}
}
