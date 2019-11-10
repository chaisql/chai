package genji

import (
	"bytes"
	"testing"

	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/record"
	"github.com/stretchr/testify/require"
)

func TestParserDelete(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected statement
	}{
		{"NoCond", "DELETE FROM test", deleteStmt{tableName: "test"}},
		{"WithCond", "DELETE FROM test WHERE age = 10", deleteStmt{tableName: "test", whereExpr: eq(fieldSelector("age"), int8Value(10))}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parseQuery(test.s)
			require.NoError(t, err)
			require.Len(t, q.Statements, 1)
			require.EqualValues(t, test.expected, q.Statements[0])
		})
	}
}

func TestDeleteStmt(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		fails    bool
		expected string
		params   []interface{}
	}{
		{"No cond", `DELETE FROM test`, false, "", nil},
		{"With cond", "DELETE FROM test WHERE b = 'bar1'", false, "foo3,bar2,bar3\n", nil},
		{"Table not found", "DELETE FROM foo WHERE b = 'bar1'", true, "", nil},
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
			err = db.Exec("INSERT INTO test (a, b) VALUES ('foo2', 'bar1')")
			require.NoError(t, err)
			err = db.Exec("INSERT INTO test (d, b, e) VALUES ('foo3', 'bar2', 'bar3')")
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
