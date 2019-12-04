package query_test

import (
	"bytes"
	"testing"

	"github.com/asdine/genji"
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/engine/memoryengine"
	"github.com/stretchr/testify/require"
)

func TestDeleteStmt(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		fails    bool
		expected string
		params   []interface{}
	}{
		{"No cond", `DELETE FROM test`, false, "", nil},
		{"With cond", "DELETE FROM test WHERE b = 'bar1'", false, "bar2,foo3,bar3\n", nil},
		{"Table not found", "DELETE FROM foo WHERE b = 'bar1'", true, "", nil},
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
			err = document.IteratorToCSV(&buf, st)
			require.NoError(t, err)
			require.Equal(t, test.expected, buf.String())
		})
	}
}
