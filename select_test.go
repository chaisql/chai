package genji

import (
	"bytes"
	"database/sql"
	"testing"

	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/record"
	"github.com/stretchr/testify/require"
)

func TestParserSelect(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected statement
		mustFail bool
	}{
		{"NoCond", "SELECT * FROM test",
			selectStmt{
				selectors: []fieldSelector{"*"},
				tableName: "test",
			}, false},
		{"WithFields", "SELECT a, b FROM test",
			selectStmt{
				selectors: []fieldSelector{"a", "b"},
				tableName: "test",
			}, false},
		{"WithFields and wildcard", "SELECT a, b, * FROM test",
			selectStmt{
				selectors: []fieldSelector{"a", "b", "*"},
				tableName: "test",
			}, false},
		{"WithCond", "SELECT * FROM test WHERE age = 10",
			selectStmt{
				tableName: "test",
				selectors: []fieldSelector{"*"},
				whereExpr: eq(fieldSelector("age"), int64Value(10)),
			}, false},
		{"WithLimit", "SELECT * FROM test WHERE age = 10 LIMIT 20",
			selectStmt{
				selectors: []fieldSelector{"*"},
				tableName: "test",
				whereExpr: eq(fieldSelector("age"), int64Value(10)),
				limitExpr: int64Value(20),
			}, false},
		{"WithOffset", "SELECT * FROM test WHERE age = 10 OFFSET 20",
			selectStmt{
				selectors:  []fieldSelector{"*"},
				tableName:  "test",
				whereExpr:  eq(fieldSelector("age"), int64Value(10)),
				offsetExpr: int64Value(20),
			}, false},
		{"WithLimitThenOffset", "SELECT * FROM test WHERE age = 10 LIMIT 10 OFFSET 20",
			selectStmt{
				selectors:  []fieldSelector{"*"},
				tableName:  "test",
				whereExpr:  eq(fieldSelector("age"), int64Value(10)),
				offsetExpr: int64Value(20),
				limitExpr:  int64Value(10),
			}, false},
		{"WithOffsetThenLimit", "SELECT * FROM test WHERE age = 10 OFFSET 20 LIMIT 10", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parseQuery(test.s)
			if !test.mustFail {
				require.NoError(t, err)
				require.Len(t, q.Statements, 1)
				require.EqualValues(t, test.expected, q.Statements[0])
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestSelectStmt(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		fails    bool
		expected string
		params   []interface{}
	}{
		{"No cond", "SELECT * FROM test", false, "foo1,bar1,baz1\nfoo2,bar1,1\nfoo3,bar2\n", nil},
		{"Multiple wildcards cond", "SELECT *, *, a FROM test", false, "foo1,bar1,baz1,foo1,bar1,baz1,foo1\nfoo2,bar1,1,foo2,bar1,1,foo2\nfoo3,bar2,foo3,bar2\n", nil},
		{"With fields", "SELECT a, c FROM test", false, "foo1,baz1\nfoo2\n\n", nil},
		{"With eq cond", "SELECT * FROM test WHERE b = 'bar1'", false, "foo1,bar1,baz1\nfoo2,bar1,1\n", nil},
		{"With gt cond", "SELECT * FROM test WHERE b > 'bar1'", false, "", nil},
		{"With limit", "SELECT * FROM test WHERE b = 'bar1' LIMIT 1", false, "foo1,bar1,baz1\n", nil},
		{"With offset", "SELECT *, _key FROM test WHERE b = 'bar1' OFFSET 1", false, "foo2,bar1,1,2\n", nil},
		{"With limit then offset", "SELECT * FROM test WHERE b = 'bar1' LIMIT 1 OFFSET 1", false, "foo2,bar1,1\n", nil},
		{"With offset then limit", "SELECT * FROM test WHERE b = 'bar1' OFFSET 1 LIMIT 1", true, "", nil},
		{"With positional params", "SELECT * FROM test WHERE a = ? OR d = ?", false, "foo1,bar1,baz1\nfoo3,bar2\n", []interface{}{"foo1", "foo3"}},
		{"With named params", "SELECT * FROM test WHERE a = $a OR d = $d", false, "foo1,bar1,baz1\nfoo3,bar2\n", []interface{}{sql.Named("a", "foo1"), sql.Named("d", "foo3")}},
		{"With _key", "SELECT _key, a FROM test", false, "1,foo1\n2,foo2\n3\n", []interface{}{sql.Named("a", "foo1"), sql.Named("d", "foo3")}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testFn := func(withIndexes bool) func(t *testing.T) {
				return func(t *testing.T) {
					db, err := New(memory.NewEngine())
					require.NoError(t, err)
					defer db.Close()

					err = db.Exec("CREATE TABLE test")
					require.NoError(t, err)
					if withIndexes {
						err = db.Exec(`
						CREATE INDEX idx_a ON test (a);
						CREATE INDEX idx_b ON test (b);
						CREATE INDEX idx_c ON test (c);
						CREATE INDEX idx_d ON test (d);
						CREATE INDEX idx_e ON test (e);
					`)
						require.NoError(t, err)
					}

					err = db.Exec("INSERT INTO test (a, b, c) VALUES ('foo1', 'bar1', 'baz1')")
					require.NoError(t, err)
					err = db.Exec("INSERT INTO test (a, b, e) VALUES ('foo2', 'bar1', 1)")
					require.NoError(t, err)
					err = db.Exec("INSERT INTO test (d, e) VALUES ('foo3', 'bar2')")
					require.NoError(t, err)

					st, err := db.Query(test.query, test.params...)
					if test.fails {
						require.Error(t, err)
						return
					}
					require.NoError(t, err)
					defer st.Close()

					var buf bytes.Buffer
					err = record.IteratorToCSV(&buf, st)
					require.NoError(t, err)
					require.Equal(t, test.expected, buf.String())
				}
			}

			t.Run("No Index/"+test.name, testFn(false))
			t.Run("With Index/"+test.name, testFn(true))
		})
	}

	t.Run("Shadow _key", func(t *testing.T) {
		db, err := New(memory.NewEngine())
		require.NoError(t, err)
		defer db.Close()

		err = db.Exec("CREATE TABLE test")
		require.NoError(t, err)

		err = db.Exec("INSERT INTO test (a, _key) VALUES ('foo', 'bar')")
		require.NoError(t, err)

		st, err := db.Query("SELECT a, _key FROM test")
		require.NoError(t, err)
		defer st.Close()

		var buf bytes.Buffer
		err = record.IteratorToCSV(&buf, st)
		require.NoError(t, err)
		require.Equal(t, "foo,bar\n", buf.String())
	})

	t.Run("with primary key", func(t *testing.T) {
		db, err := New(memory.NewEngine())
		require.NoError(t, err)
		defer db.Close()

		err = db.Exec("CREATE TABLE test WITH PRIMARY KEY foo")
		require.NoError(t, err)

		err = db.Exec(`INSERT INTO test (foo, bar) VALUES (1, "a")`)
		require.NoError(t, err)

		st, err := db.Query("SELECT foo FROM test")
		require.NoError(t, err)
		defer st.Close()

		var buf bytes.Buffer
		err = record.IteratorToCSV(&buf, st)
		require.NoError(t, err)
		require.Equal(t, "1\n", buf.String())
	})
}
