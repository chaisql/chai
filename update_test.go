package genji

import (
	"bytes"
	"database/sql"
	"testing"
	"time"

	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/record"
	"github.com/stretchr/testify/require"
)

func TestParserUdpate(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected statement
		errored  bool
	}{
		{"No cond", "UPDATE test SET a = 1",
			updateStmt{
				tableName: "test",
				pairs: map[string]expr{
					"a": int64Value(1),
				},
			},
			false},
		{"With cond", "UPDATE test SET a = 1, b = 2 WHERE age = 10",
			updateStmt{
				tableName: "test",
				pairs: map[string]expr{
					"a": int64Value(1),
					"b": int64Value(2),
				},
				whereExpr: eq(fieldSelector("age"), int64Value(10)),
			},
			false},
		{"Trailing comma", "UPDATE test SET a = 1, WHERE age = 10", nil, true},
		{"No SET", "UPDATE test WHERE age = 10", nil, true},
		{"No pair", "UPDATE test SET WHERE age = 10", nil, true},
		{"query.Field only", "UPDATE test SET a WHERE age = 10", nil, true},
		{"No value", "UPDATE test SET a = WHERE age = 10", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parseQuery(test.s)
			if test.errored {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, q.Statements, 1)
			require.EqualValues(t, test.expected, q.Statements[0])
		})
	}
}

func TestUpdateStmt(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		fails    bool
		expected string
		params   []interface{}
	}{
		{"No cond", `UPDATE test SET a = 'boo'`, false, "boo,bar1,baz1\nboo,bar2\nfoo3,bar3\n", nil},
		{"With cond", "UPDATE test SET a = 1, b = 2 WHERE a = 'foo2'", false, "foo1,bar1,baz1\n1,2\nfoo3,bar3\n", nil},
		{"Field not found", "UPDATE test SET a = 1, b = 2 WHERE a = f", false, "foo1,bar1,baz1\nfoo2,bar2\nfoo3,bar3\n", nil},
		{"Positional params", "UPDATE test SET a = ?, b = ? WHERE a = ?", false, "a,b,baz1\nfoo2,bar2\nfoo3,bar3\n", []interface{}{"a", "b", "foo1"}},
		{"Named params", "UPDATE test SET a = $a, b = $b WHERE a = $c", false, "a,b,baz1\nfoo2,bar2\nfoo3,bar3\n", []interface{}{sql.Named("b", "b"), sql.Named("a", "a"), sql.Named("c", "foo1")}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := New(memory.NewEngine())
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec("CREATE TABLE test")
			require.NoError(t, err)
			err = db.Exec("INSERT INTO test (a, b, c) VALUES ('foo1', 'bar1', 'baz1')")
			require.NoError(t, err)
			time.Sleep(time.Millisecond)
			err = db.Exec("INSERT INTO test (a, b) VALUES ('foo2', 'bar2')")
			require.NoError(t, err)
			time.Sleep(time.Millisecond)
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
			err = record.IteratorToCSV(&buf, st)
			require.NoError(t, err)
			require.Equal(t, test.expected, buf.String())
		})
	}
}
