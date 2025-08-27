package statement_test

import (
	"database/sql"
	"testing"

	_ "github.com/chaisql/chai"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestUpdateStmt(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		fails    bool
		expected string
		params   []any
	}{
		{"No clause", `UPDATE test`, true, "", nil},
		{"Read-only table", `UPDATE __chai_catalog SET a = 1`, true, "", nil},

		{"SET / No cond", `UPDATE test SET a = 'boo'`, false, `[{"a":"boo","b":"bar1","c":"baz1","d":null,"e":null},{"a":"boo","b":"bar2","c":null,"d":null,"e":null},{"a":"boo","b":null,"c":null,"d":"bar3","e":"baz3"}]`, nil},
		{"SET / No cond / with ident string", "UPDATE test SET `a` = 'boo'", false, `[{"a":"boo","b":"bar1","c":"baz1","d":null,"e":null},{"a":"boo","b":"bar2","c":null,"d":null,"e":null},{"a":"boo","b":null,"c":null,"d":"bar3","e":"baz3"}]`, nil},
		{"SET / No cond / with multiple idents and constraint", `UPDATE test SET a = c`, true, ``, nil},
		{"SET / No cond / with multiple idents", `UPDATE test SET b = c`, false, `[{"a":"foo1","b":"baz1","c":"baz1","d":null,"e":null},{"a":"foo2","b":null,"c":null,"d":null,"e":null},{"a":"foo3","b":null,"c":null,"d":"bar3","e":"baz3"}]`, nil},
		{"SET / No cond / with missing column", "UPDATE test SET f = 'boo'", true, "", nil},
		{"SET / No cond / with string", `UPDATE test SET 'a' = 'boo'`, true, "", nil},
		{"SET / With cond", "UPDATE test SET a = 'FOO2', b = 2 WHERE a = 'foo2'", false, `[{"a":"foo1","b":"bar1","c":"baz1","d":null,"e":null},{"a":"FOO2","b":"2","c":null,"d":null,"e":null},{"a":"foo3","b":null,"c":null,"d":"bar3","e":"baz3"}]`, nil},
		{"SET / With cond / with missing column", "UPDATE test SET f = 'boo' WHERE d = 'bar3'", true, ``, nil},
		{"SET / Field not found", "UPDATE test SET a = 1, b = 2 WHERE a = f", true, ``, nil},
		{"SET / Positional params", "UPDATE test SET a = $1, b = $2 WHERE a = $3", false, `[{"a":"a","b":"b","c":"baz1","d":null,"e":null},{"a":"foo2","b":"bar2","c":null,"d":null,"e":null},{"a":"foo3","b":null,"c":null,"d":"bar3","e":"baz3"}]`, []interface{}{"a", "b", "foo1"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runTest := func(indexed bool) {
				db, err := sql.Open("chai", ":memory:")
				require.NoError(t, err)
				defer db.Close()

				_, err = db.Exec("CREATE TABLE test (a text not null, b text, c text, d text, e text)")
				require.NoError(t, err)

				if indexed {
					_, err = db.Exec("CREATE INDEX idx_test_a ON test(a)")
					require.NoError(t, err)
				}

				_, err = db.Exec("INSERT INTO test (a, b, c) VALUES ('foo1', 'bar1', 'baz1')")
				require.NoError(t, err)
				_, err = db.Exec("INSERT INTO test (a, b) VALUES ('foo2', 'bar2')")
				require.NoError(t, err)
				_, err = db.Exec("INSERT INTO test (a, d, e) VALUES ('foo3', 'bar3', 'baz3')")
				require.NoError(t, err)

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

			runTest(false)
			runTest(true)
		})
	}
}
