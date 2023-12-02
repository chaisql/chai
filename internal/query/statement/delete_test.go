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
		{"With cond", "DELETE FROM test WHERE b = 'bar1'", false, `[{"d": "foo3", "b": "bar2", "e": "bar3", "n": 1}]`, nil},
		{"With offset", "DELETE FROM test OFFSET 1", false, `[{"a":"foo1", "b":"bar1", "c":"baz1", "n": 3}]`, nil},
		{"With order by then offset", "DELETE FROM test ORDER BY n OFFSET 1", false, `[{"d":"foo3", "b":"bar2", "e":"bar3", "n": 1}]`, nil},
		{"With order by DESC then offset", "DELETE FROM test ORDER BY n DESC OFFSET 1", false, `[{"a": "foo1", "b": "bar1", "c": "baz1", "n": 3}]`, nil},
		{"With limit", "DELETE FROM test ORDER BY n LIMIT 2", false, `[{"a":"foo1", "b":"bar1", "c":"baz1", "n": 3}]`, nil},
		{"With order by then limit then offset", "DELETE FROM test ORDER BY n LIMIT 1 OFFSET 1", false, `[{"a": "foo1", "b": "bar1", "c": "baz1", "n": 3}, {"d": "foo3", "b": "bar2", "e": "bar3", "n": 1}]`, nil},
		{"Table not found", "DELETE FROM foo WHERE b = 'bar1'", true, "[]", nil},
		{"Read-only table", "DELETE FROM __chai_catalog", true, "[]", nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := chai.Open(":memory:")
			assert.NoError(t, err)
			defer db.Close()

			err = db.Exec("CREATE TABLE test")
			assert.NoError(t, err)
			err = db.Exec("INSERT INTO test (a, b, c, n) VALUES ('foo1', 'bar1', 'baz1', 3)")
			assert.NoError(t, err)
			err = db.Exec("INSERT INTO test (a, b, n) VALUES ('foo2', 'bar1', 2)")
			assert.NoError(t, err)
			err = db.Exec("INSERT INTO test (d, b, e, n) VALUES ('foo3', 'bar2', 'bar3', 1)")
			assert.NoError(t, err)

			err = db.Exec(test.query, test.params...)
			if test.fails {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			st, err := db.Query("SELECT * FROM test")
			assert.NoError(t, err)
			defer st.Close()

			var buf bytes.Buffer
			err = st.MarshalJSONTo(&buf)
			assert.NoError(t, err)
			require.JSONEq(t, test.expected, buf.String())
		})
	}
}
