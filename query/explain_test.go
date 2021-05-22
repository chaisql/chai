package query_test

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
		{"EXPLAIN SELECT 1 + 1", false, `"project(1 + 1)"`},
		{"EXPLAIN SELECT * FROM noexist", true, ``},
		{"EXPLAIN SELECT * FROM test", false, `"seqScan(test)"`},
		{"EXPLAIN SELECT *, a FROM test", false, `"seqScan(test) | project(*, a)"`},
		{"EXPLAIN SELECT a + 1 FROM test", false, `"seqScan(test) | project(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c > 10", false, `"seqScan(test) | filter(c > 10) | project(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c > 10 AND d > 20", false, `"seqScan(test) | filter(c > 10) | filter(d > 20) | project(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c > 10 OR d > 20", false, `"seqScan(test) | filter(c > 10 OR d > 20) | project(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c IN [1 + 1, 2 + 2]", false, `"seqScan(test) | filter(c IN [2, 4]) | project(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE a > 10", false, `"indexScan(\"idx_a\", [10, -1, true]) | project(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE x = 10 AND y > 5", false, `"indexScan(\"idx_x_y\", [[10, 5], -1, true]) | project(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE a > 10 AND b > 20 AND c > 30", false, `"indexScan(\"idx_b\", [20, -1, true]) | filter(a > 10) | filter(c > 30) | project(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c > 30 ORDER BY d LIMIT 10 OFFSET 20", false, `"seqScan(test) | filter(c > 30) | project(a + 1) | sort(d) | skip(20) | take(10)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c > 30 ORDER BY d DESC LIMIT 10 OFFSET 20", false, `"seqScan(test) | filter(c > 30) | project(a + 1) | sortReverse(d) | skip(20) | take(10)"`},
		// {"EXPLAIN SELECT a + 1 FROM test WHERE c > 30 ORDER BY a DESC LIMIT 10 OFFSET 20", false, `"indexScanReverse(\"idx_a\") | filter(c > 30) | project(a + 1) | skip(20) | take(10)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c > 30 ORDER BY a DESC LIMIT 10 OFFSET 20", false, `"seqScan(test) | filter(c > 30) | project(a + 1) | sortReverse(a) | skip(20) | take(10)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c > 30 GROUP BY a + 1 ORDER BY a DESC LIMIT 10 OFFSET 20", false, `"seqScan(test) | filter(c > 30) | groupBy(a + 1) | hashAggregate() | project(a + 1) | sortReverse(a) | skip(20) | take(10)"`},
		{"EXPLAIN UPDATE test SET a = 10", false, `"seqScan(test) | set(a, 10) | tableReplace('test')"`},
		{"EXPLAIN UPDATE test SET a = 10 WHERE c > 10", false, `"seqScan(test) | filter(c > 10) | set(a, 10) | tableReplace('test')"`},
		{"EXPLAIN UPDATE test SET a = 10 WHERE a > 10", false, `"indexScan(\"idx_a\", [10, -1, true]) | set(a, 10) | tableReplace('test')"`},
		{"EXPLAIN DELETE FROM test", false, `"seqScan(test) | tableDelete('test')"`},
		{"EXPLAIN DELETE FROM test WHERE c > 10", false, `"seqScan(test) | filter(c > 10) | tableDelete('test')"`},
		{"EXPLAIN DELETE FROM test WHERE a > 10", false, `"indexScan(\"idx_a\", [10, -1, true]) | tableDelete('test')"`},
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
						CREATE INDEX idx_x_y ON test (x, y);
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
