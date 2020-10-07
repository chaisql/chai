package query_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/stretchr/testify/require"
)

func TestDeleteStmt(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name     string
		query    string
		fails    bool
		expected string
		params   []interface{}
	}{
		{"No cond", `DELETE FROM test`, false, "", nil},
		{"With cond", "DELETE FROM test WHERE b = 'bar1'", false, `{"d": "foo3", "b": "bar2", "e": "bar3"}`, nil},
		{"Table not found", "DELETE FROM foo WHERE b = 'bar1'", true, "", nil},
		{"Read-only table", "DELETE FROM __genji_tables", true, "", nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.Open(ctx, ":memory:")
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec(ctx, "CREATE TABLE test")
			require.NoError(t, err)
			err = db.Exec(ctx, "INSERT INTO test (a, b, c) VALUES ('foo1', 'bar1', 'baz1')")
			require.NoError(t, err)
			err = db.Exec(ctx, "INSERT INTO test (a, b) VALUES ('foo2', 'bar1')")
			require.NoError(t, err)
			err = db.Exec(ctx, "INSERT INTO test (d, b, e) VALUES ('foo3', 'bar2', 'bar3')")
			require.NoError(t, err)

			err = db.Exec(ctx, test.query, test.params...)
			if test.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			st, err := db.Query(ctx, "SELECT * FROM test")
			require.NoError(t, err)
			defer st.Close()

			var buf bytes.Buffer
			err = document.IteratorToJSON(ctx, &buf, st)
			require.NoError(t, err)
			if len(test.expected) == 0 {
				require.Equal(t, 0, buf.Len())
			} else {
				require.JSONEq(t, test.expected, buf.String())
			}
		})
	}
}
