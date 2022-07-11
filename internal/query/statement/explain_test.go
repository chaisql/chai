package statement_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/internal/testutil/assert"
)

func TestExplainStmt(t *testing.T) {
	tests := []struct {
		query    string
		fails    bool
		expected string
	}{
		{"EXPLAIN SELECT 1 + 1", false, `"docs.Project(1 + 1)"`},
		{"EXPLAIN SELECT * FROM noexist", true, ``},
		{"EXPLAIN SELECT * FROM test", false, `"table.Scan(\"test\")"`},
		{"EXPLAIN SELECT *, a FROM test", false, `"table.Scan(\"test\") | docs.Project(*, a)"`},
		{"EXPLAIN SELECT a + 1 FROM test", false, `"table.Scan(\"test\") | docs.Project(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c > 10", true, ``},
		{"EXPLAIN SELECT a + 1 FROM test WHERE a > 10 AND d > 20", true, ``},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c > 10 OR d > 20", true, ``},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c IN [1 + 1, 2 + 2]", true, ``},
		{"EXPLAIN SELECT a + 1 FROM test WHERE a > 10", false, `"index.Scan(\"idx_a\", [{\"min\": [10], \"exclusive\": true}]) | docs.Project(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE x = 10 AND y > 5", false, `"index.Scan(\"idx_x_y\", [{\"min\": [10, 5], \"exclusive\": true}]) | docs.Project(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE a > 10 AND b > 20 AND c > 30", true, ``},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c > 30 ORDER BY d LIMIT 10 OFFSET 20", true, ``},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c > 30 ORDER BY d DESC LIMIT 10 OFFSET 20", true, ``},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c > 30 ORDER BY a DESC LIMIT 10 OFFSET 20", true, ``},
		{"EXPLAIN SELECT a FROM test WHERE c > 30 GROUP BY a ORDER BY a DESC LIMIT 10 OFFSET 20", true, ``},
		{"EXPLAIN SELECT a FROM test WHERE a > 30 GROUP BY c", true, ``},
		{"EXPLAIN SELECT a FROM test WHERE a > 30 GROUP BY a ORDER BY c", true, ``},
		{"EXPLAIN UPDATE test SET a = 10", false, `"table.Scan(\"test\") | paths.Set(a, 10) | table.Validate(\"test\") | index.Delete(\"idx_a\") | index.Delete(\"idx_b\") | index.Delete(\"idx_x_y\") | table.Replace(\"test\") | index.Insert(\"idx_a\") | index.Insert(\"idx_b\") | index.Insert(\"idx_x_y\") | discard()"`},
		{"EXPLAIN UPDATE test SET a = 10 WHERE c > 10", false, `"table.Scan(\"test\") | docs.Filter(c > 10) | paths.Set(a, 10) | table.Validate(\"test\") | index.Delete(\"idx_a\") | index.Delete(\"idx_b\") | index.Delete(\"idx_x_y\") | table.Replace(\"test\") | index.Insert(\"idx_a\") | index.Insert(\"idx_b\") | index.Insert(\"idx_x_y\") | discard()"`},
		{"EXPLAIN UPDATE test SET a = 10 WHERE a > 10", false, `"index.Scan(\"idx_a\", [{\"min\": [10], \"exclusive\": true}]) | paths.Set(a, 10) | table.Validate(\"test\") | index.Delete(\"idx_a\") | index.Delete(\"idx_b\") | index.Delete(\"idx_x_y\") | table.Replace(\"test\") | index.Insert(\"idx_a\") | index.Insert(\"idx_b\") | index.Insert(\"idx_x_y\") | discard()"`},
		{"EXPLAIN DELETE FROM test", false, `"table.Scan(\"test\") | index.Delete(\"idx_a\") | index.Delete(\"idx_b\") | index.Delete(\"idx_x_y\") | table.Delete('test') | discard()"`},
		{"EXPLAIN DELETE FROM test WHERE c > 10", false, `"table.Scan(\"test\") | docs.Filter(c > 10) | index.Delete(\"idx_a\") | index.Delete(\"idx_b\") | index.Delete(\"idx_x_y\") | table.Delete('test') | discard()"`},
		{"EXPLAIN DELETE FROM test WHERE a > 10", false, `"index.Scan(\"idx_a\", [{\"min\": [10], \"exclusive\": true}]) | index.Delete(\"idx_a\") | index.Delete(\"idx_b\") | index.Delete(\"idx_x_y\") | table.Delete('test') | discard()"`},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			assert.NoError(t, err)
			defer db.Close()

			err = db.Exec("CREATE TABLE test (k INTEGER PRIMARY KEY, a any, b any, x any, y any)")
			assert.NoError(t, err)
			err = db.Exec(`
						CREATE INDEX idx_a ON test (a);
						CREATE UNIQUE INDEX idx_b ON test (b);
						CREATE INDEX idx_x_y ON test (x, y);
					`)
			assert.NoError(t, err)

			d, err := db.QueryDocument(test.query)
			if test.fails {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			v, err := d.GetByField("plan")
			assert.NoError(t, err)

			got, err := json.Marshal(v)
			assert.NoError(t, err)

			require.JSONEq(t, test.expected, string(got))
		})
	}
}
