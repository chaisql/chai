package query_test

import (
	"bytes"
	"database/sql"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/internal/testutil"
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
		{"Read-only table", `UPDATE __genji_tables SET a = 1`, true, "", nil},

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
			runTest := func(indexed bool) {
				db, err := genji.Open(":memory:")
				require.NoError(t, err)
				defer db.Close()

				err = db.Exec("CREATE TABLE test (a text not null)")
				require.NoError(t, err)

				if indexed {
					err = db.Exec("CREATE INDEX idx_test_a ON test(a)")
					require.NoError(t, err)
				}

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

				err = testutil.IteratorToJSONArray(&buf, st)
				require.NoError(t, err)
				require.JSONEq(t, test.expected, buf.String())
			}

			runTest(false)
			runTest(true)
		})
	}

	t.Run("with arrays", func(t *testing.T) {
		tests := []struct {
			name     string
			query    string
			fails    bool
			expected string
			params   []interface{}
		}{
			{"SET / No cond add field ", `UPDATE foo set b = 0`, false, `[{"a": [1, 0, 0], "b": 0}, {"a": [2, 0], "b": 0}]`, nil},
			{"SET / No cond / with path at existing index only", `UPDATE foo SET a[2] = 10`, false, `[{"a": [1, 0, 10]}, {"a": [2, 0]}]`, nil},
			{"SET / No cond / with index array", `UPDATE foo SET a[1] = 10`, false, `[{"a": [1, 10, 0]}, {"a": [2, 10]}]`, nil},
			{"SET / No cond / with path on non existing field", `UPDATE foo SET a.foo[1] = 10`, false, `[{"a": [1, 0, 0]}, {"a": [2, 0]}]`, nil},
			{"SET / With cond / index array", `UPDATE foo SET a[0] = 1 WHERE a[0] = 2`, false, `[{"a": [1, 0, 0]}, {"a": [1, 0]}]`, nil},
			{"SET / No cond / index out of range", `UPDATE foo SET a[10] = 1`, false, `[{"a": [1, 0, 0]}, {"a": [2, 0]}]`, nil},
			{"SET / No cond / Nested array", `UPDATE foo SET a[1] = [1, 0, 0]`, false, `[{"a": [1, [1, 0, 0], 0]}, {"a": [2, [1, 0, 0]]}]`, nil},
			{"SET / No cond / with multiple idents", `UPDATE foo SET a[1] = [1, 0, 0], a[1][2] = 9`, false, `[{"a": [1, [1, 0, 9], 0]}, {"a": [2, [1, 0, 9]]}]`, nil},
			{"SET / No cond / add doc / with multiple idents with multiple indexes", `UPDATE foo SET a[1] = [1, 0, 0], a[1][2] = {"b": "foo"}`, false, `[{"a": [1, [1, 0, {"b":"foo"}], 0]}, {"a": [2, [1, 0, {"b":"foo"}]]}]`, nil},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				db, err := genji.Open(":memory:")
				require.NoError(t, err)
				defer db.Close()

				err = db.Exec(`CREATE TABLE foo;`)
				require.NoError(t, err)
				err = db.Exec(`INSERT INTO foo (a) VALUES ([1, 0, 0]), ([2, 0]);`)
				require.NoError(t, err)

				err = db.Exec(tt.query, tt.params...)
				if tt.fails {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)

				st, err := db.Query("SELECT * FROM foo")
				require.NoError(t, err)
				defer st.Close()

				var buf bytes.Buffer

				err = testutil.IteratorToJSONArray(&buf, st)
				require.NoError(t, err)
				require.JSONEq(t, tt.expected, buf.String())
			})
		}
	})
}
