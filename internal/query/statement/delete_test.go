package statement_test

import (
	"bytes"
	"testing"

	"github.com/chaisql/chai"
	"github.com/chaisql/chai/internal/testutil/assert"
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
		{"No cond", `DELETE FROM test`, false, "[]", nil},
		{"With cond", "DELETE FROM test WHERE b = 'bar1'", false, `[{"id": 3}]`, nil},
		{"With offset", "DELETE FROM test OFFSET 1", false, `[{"id":1}]`, nil},
		{"With order by then offset", "DELETE FROM test ORDER BY n OFFSET 1", false, `[{"id": 3}]`, nil},
		{"With order by DESC then offset", "DELETE FROM test ORDER BY n DESC OFFSET 1", false, `[{"id":1}]`, nil},
		{"With limit", "DELETE FROM test ORDER BY n LIMIT 2", false, `[{"id":1}]`, nil},
		{"With order by then limit then offset", "DELETE FROM test ORDER BY n LIMIT 1 OFFSET 1", false, `[{"id":1}, {"id": 3}]`, nil},
		{"Table not found", "DELETE FROM foo WHERE b = 'bar1'", true, "[]", nil},
		{"Read-only table", "DELETE FROM __chai_catalog", true, "[]", nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := chai.Open(":memory:")
			assert.NoError(t, err)
			defer db.Close()

			err = db.Exec("CREATE TABLE test(id INT PRIMARY KEY, a TEXT, b TEXT, c TEXT, d TEXT, e TEXT, n INT)")
			assert.NoError(t, err)
			err = db.Exec("INSERT INTO test (id, a, b, c, n) VALUES (1, 'foo1', 'bar1', 'baz1', 3)")
			assert.NoError(t, err)
			err = db.Exec("INSERT INTO test (id, a, b, n) VALUES (2, 'foo2', 'bar1', 2)")
			assert.NoError(t, err)
			err = db.Exec("INSERT INTO test (id, d, b, e, n) VALUES (3, 'foo3', 'bar2', 'bar3', 1)")
			assert.NoError(t, err)

			err = db.Exec(test.query, test.params...)
			if test.fails {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			st, err := db.Query("SELECT id FROM test")
			assert.NoError(t, err)
			defer st.Close()

			var buf bytes.Buffer
			err = st.MarshalJSONTo(&buf)
			assert.NoError(t, err)
			require.JSONEq(t, test.expected, buf.String())
		})
	}
}
