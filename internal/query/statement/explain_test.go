package statement_test

import (
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExplainStmt(t *testing.T) {
	tests := []struct {
		query    string
		fails    bool
		expected string
	}{
		{"EXPLAIN SELECT 1 + 1", false, `"rows.Project(1 + 1)"`},
		{"EXPLAIN SELECT * FROM noexist", true, ``},
		{"EXPLAIN SELECT * FROM test", false, `"table.Scan(\"test\")"`},
		{"EXPLAIN SELECT *, a FROM test", false, `"table.Scan(\"test\") | rows.Project(*, a)"`},
		{"EXPLAIN SELECT a + 1 FROM test", false, `"table.Scan(\"test\") | rows.Project(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c > 10", false, `"table.Scan(\"test\") | rows.Filter(c > 10) | rows.Project(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c > 10 AND d > 20", false, `"table.Scan(\"test\") | rows.Filter(c > 10) | rows.Filter(d > 20) | rows.Project(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c > 10 OR d > 20", false, `"table.Scan(\"test\") | rows.Filter(c > 10 OR d > 20) | rows.Project(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c IN (1 + 1, 2 + 2)", false, `"table.Scan(\"test\") | rows.Filter(c IN (2, 4)) | rows.Project(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE a > 10", false, `"index.Scan(\"idx_a\", [{\"min\": (10), \"exclusive\": true}]) | rows.Project(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE x = 10 AND y > 5", false, `"index.Scan(\"idx_x_y\", [{\"min\": (10, 5), \"exclusive\": true}]) | rows.Project(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE a > 10 AND b > 20 AND c > 30", false, `"index.Scan(\"idx_b\", [{\"min\": (20), \"exclusive\": true}]) | rows.Filter(a > 10) | rows.Filter(c > 30) | rows.Project(a + 1)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c > 30 ORDER BY d LIMIT 10 OFFSET 20", false, `"table.Scan(\"test\") | rows.Filter(c > 30) | rows.Project(a + 1) | rows.TempTreeSort(d) | rows.Skip(20) | rows.Take(10)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c > 30 ORDER BY d DESC LIMIT 10 OFFSET 20", false, `"table.Scan(\"test\") | rows.Filter(c > 30) | rows.Project(a + 1) | rows.TempTreeSortReverse(d) | rows.Skip(20) | rows.Take(10)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c > 30 ORDER BY a DESC LIMIT 10 OFFSET 20", false, `"index.ScanReverse(\"idx_a\") | rows.Filter(c > 30) | rows.Project(a + 1) | rows.Skip(20) | rows.Take(10)"`},
		{"EXPLAIN SELECT a FROM test WHERE c > 30 GROUP BY a ORDER BY a DESC LIMIT 10 OFFSET 20", false, `"index.ScanReverse(\"idx_a\") | rows.Filter(c > 30) | rows.GroupAggregate(a) | rows.Project(a) | rows.Skip(20) | rows.Take(10)"`},
		{"EXPLAIN SELECT a + 1 FROM test WHERE c > 30 GROUP BY a + 1 ORDER BY a DESC LIMIT 10 OFFSET 20", false, `"table.Scan(\"test\") | rows.Filter(c > 30) | rows.TempTreeSort(a + 1) | rows.GroupAggregate(a + 1) | rows.Project(a + 1) | rows.TempTreeSortReverse(a) | rows.Skip(20) | rows.Take(10)"`},
		{"EXPLAIN UPDATE test SET a = 10", false, `"table.Scan(\"test\") | paths.Set(a, 10) | table.Validate(\"test\") | index.Delete(\"idx_a\") | index.Delete(\"idx_b\") | index.Delete(\"idx_x_y\") | table.Replace(\"test\") | index.Insert(\"idx_a\") | index.Validate(\"idx_b\") | index.Insert(\"idx_b\") | index.Insert(\"idx_x_y\") | discard()"`},
		{"EXPLAIN UPDATE test SET a = 10 WHERE c > 10", false, `"table.Scan(\"test\") | rows.Filter(c > 10) | paths.Set(a, 10) | table.Validate(\"test\") | index.Delete(\"idx_a\") | index.Delete(\"idx_b\") | index.Delete(\"idx_x_y\") | table.Replace(\"test\") | index.Insert(\"idx_a\") | index.Validate(\"idx_b\") | index.Insert(\"idx_b\") | index.Insert(\"idx_x_y\") | discard()"`},
		{"EXPLAIN UPDATE test SET a = 10 WHERE a > 10", false, `"index.Scan(\"idx_a\", [{\"min\": (10), \"exclusive\": true}]) | paths.Set(a, 10) | table.Validate(\"test\") | index.Delete(\"idx_a\") | index.Delete(\"idx_b\") | index.Delete(\"idx_x_y\") | table.Replace(\"test\") | index.Insert(\"idx_a\") | index.Validate(\"idx_b\") | index.Insert(\"idx_b\") | index.Insert(\"idx_x_y\") | discard()"`},
		{"EXPLAIN DELETE FROM test", false, `"table.Scan(\"test\") | index.Delete(\"idx_a\") | index.Delete(\"idx_b\") | index.Delete(\"idx_x_y\") | table.Delete('test') | discard()"`},
		{"EXPLAIN DELETE FROM test WHERE c > 10", false, `"table.Scan(\"test\") | rows.Filter(c > 10) | index.Delete(\"idx_a\") | index.Delete(\"idx_b\") | index.Delete(\"idx_x_y\") | table.Delete('test') | discard()"`},
		{"EXPLAIN DELETE FROM test WHERE a > 10", false, `"index.Scan(\"idx_a\", [{\"min\": (10), \"exclusive\": true}]) | index.Delete(\"idx_a\") | index.Delete(\"idx_b\") | index.Delete(\"idx_x_y\") | table.Delete('test') | discard()"`},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			db, err := sql.Open("chai", ":memory:")
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE TABLE test (k INTEGER PRIMARY KEY, a INT, b INT, c INT, d INT, x INT, y INT)")
			require.NoError(t, err)
			_, err = db.Exec(`
				CREATE INDEX idx_a ON test (a);
				CREATE UNIQUE INDEX idx_b ON test (b);
				CREATE INDEX idx_x_y ON test (x, y);
			`)
			require.NoError(t, err)

			var plan string
			err = db.QueryRow(test.query).Scan(&plan)
			if test.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			got, err := json.Marshal(plan)
			require.NoError(t, err)

			require.JSONEq(t, test.expected, string(got))
		})
	}
}
