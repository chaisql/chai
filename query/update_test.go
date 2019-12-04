package query_test

import (
	"bytes"
	"database/sql"
	"testing"

	"github.com/asdine/genji"
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/engine/memoryengine"
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
		{"No cond", `UPDATE test SET a = 'boo'`, false, "boo,bar1,baz1\nboo,bar2\nfoo3,bar3\n", nil},
		{"No cond / with ident string", `UPDATE test SET "a" = 'boo'`, false, "boo,bar1,baz1\nboo,bar2\nfoo3,bar3\n", nil},
		{"No cond / with multiple idents", `UPDATE test SET a = c`, false, "baz1,bar1,baz1\nNULL,bar2\nfoo3,bar3\n", nil},
		{"No cond / with string", `UPDATE test SET 'a' = 'boo'`, true, "", nil},
		{"With cond", "UPDATE test SET a = 1, b = 2 WHERE a = 'foo2'", false, "foo1,bar1,baz1\n1,2\nfoo3,bar3\n", nil},
		{"Field not found", "UPDATE test SET a = 1, b = 2 WHERE a = f", false, "foo1,bar1,baz1\nfoo2,bar2\nfoo3,bar3\n", nil},
		{"Positional params", "UPDATE test SET a = ?, b = ? WHERE a = ?", false, "a,b,baz1\nfoo2,bar2\nfoo3,bar3\n", []interface{}{"a", "b", "foo1"}},
		{"Named params", "UPDATE test SET a = $a, b = $b WHERE a = $c", false, "a,b,baz1\nfoo2,bar2\nfoo3,bar3\n", []interface{}{sql.Named("b", "b"), sql.Named("a", "a"), sql.Named("c", "foo1")}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.New(memoryengine.NewEngine())
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
			err = document.IteratorToCSV(&buf, st)
			require.NoError(t, err)
			require.Equal(t, test.expected, buf.String())
		})
	}
}
