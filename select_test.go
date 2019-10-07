package genji

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParserSelect(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected statement
		mustFail bool
	}{
		{"NoCond", "SELECT * FROM test",
			selectStmt{
				tableName: "test",
			}, false},
		{"WithFields", "SELECT a, b FROM test",
			selectStmt{
				FieldSelectors: []fieldSelector{fieldSelector("a"), fieldSelector("b")},
				tableName:      "test",
			}, false},
		{"WithCond", "SELECT * FROM test WHERE age = 10",
			selectStmt{
				tableName: "test",
				whereExpr: eq(fieldSelector("age"), int64Value(10)),
			}, false},
		{"WithLimit", "SELECT * FROM test WHERE age = 10 LIMIT 20",
			selectStmt{
				tableName: "test",
				whereExpr: eq(fieldSelector("age"), int64Value(10)),
				limitExpr: int64Value(20),
			}, false},
		{"WithOffset", "SELECT * FROM test WHERE age = 10 OFFSET 20",
			selectStmt{
				tableName:  "test",
				whereExpr:  eq(fieldSelector("age"), int64Value(10)),
				offsetExpr: int64Value(20),
			}, false},
		{"WithLimitThenOffset", "SELECT * FROM test WHERE age = 10 LIMIT 10 OFFSET 20",
			selectStmt{
				tableName:  "test",
				whereExpr:  eq(fieldSelector("age"), int64Value(10)),
				offsetExpr: int64Value(20),
				limitExpr:  int64Value(10),
			}, false},
		{"WithOffsetThenLimit", "SELECT * FROM test WHERE age = 10 OFFSET 20 LIMIT 10", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parseQuery(test.s)
			if !test.mustFail {
				require.NoError(t, err)
				require.Len(t, q.Statements, 1)
				require.EqualValues(t, test.expected, q.Statements[0])
			} else {
				require.Error(t, err)
			}
		})
	}
}
