package planner_test

import (
	"testing"

	"github.com/genjidb/genji"
	"github.com/stretchr/testify/require"
)

func TestExplainStmt(t *testing.T) {
	tests := []struct {
		query    string
		fails    bool
		expected string
	}{
		{"EXPLAIN SELECT 1 + 1", false, `"∏(1 + 1)"`},
		{"EXPLAIN SELECT * FROM noexist", true, ``},
		{"EXPLAIN SELECT * FROM test", false, `"Table(test) -> ∏(*)"`},
		{"EXPLAIN SELECT a + 1 FROM test", false, `"Table(test) -> ∏(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c > 10", false, `"Table(test) -> σ(cond: c > 10) -> ∏(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c > 10 AND d > 20", false, `"Table(test) -> σ(cond: d > 20) -> σ(cond: c > 10) -> ∏(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c > 10 OR d > 20", false, `"Table(test) -> σ(cond: c > 10 OR d > 20) -> ∏(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c IN [1 + 1, 2 + 2]", false, `"Table(test) -> σ(cond: c IN [2, 4]) -> ∏(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE a > 10", false, `"Index(idx_a) -> ∏(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE a > 10 AND b > 20 AND c > 30", false, `"Index(idx_b) -> σ(cond: c > 30) -> σ(cond: a > 10) -> ∏(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c > 30 ORDER BY a DESC LIMIT 10 OFFSET 20", false, `"Table(test) -> σ(cond: c > 30) -> Sort(a DESC) -> Offset(20) -> Limit(10) -> ∏(a + 1)"`},
		{"EXPLAIN UPDATE test SET a = 10", false, `"Table(test) -> Set(a = 10) -> Replace(test)"`},
		{"EXPLAIN UPDATE test SET a = 10 WHERE c > 10", false, `"Table(test) -> σ(cond: c > 10) -> Set(a = 10) -> Replace(test)"`},
		{"EXPLAIN UPDATE test SET a = 10 WHERE a > 10", false, `"Index(idx_a) -> Set(a = 10) -> Replace(test)"`},
		{"EXPLAIN DELETE FROM test", false, `"Table(test) -> Delete(test)"`},
		{"EXPLAIN DELETE FROM test WHERE c > 10", false, `"Table(test) -> σ(cond: c > 10) -> Delete(test)"`},
		{"EXPLAIN DELETE FROM test WHERE a > 10", false, `"Index(idx_a) -> Delete(test)"`},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec("CREATE TABLE test (k INTEGER PRIMARY KEY)")
			require.NoError(t, err)
			err = db.Exec(`
						CREATE INDEX idx_a ON test (a);
						CREATE UNIQUE INDEX idx_b ON test (b);
					`)
			require.NoError(t, err)

			d, err := db.QueryDocument(test.query)
			if test.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			v, err := d.GetByField("plan")
			require.NoError(t, err)

			require.JSONEq(t, test.expected, v.String())
		})
	}
}
