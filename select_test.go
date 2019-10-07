package genji

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParserSelect(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected Statement
		mustFail bool
	}{
		{"NoCond", "SELECT * FROM test",
			selectStmt{
				tableSelector: tableSelector("test"),
			}, false},
		{"WithFields", "SELECT a, b FROM test",
			selectStmt{
				FieldSelectors: []FieldSelector{FieldSelector("a"), FieldSelector("b")},
				tableSelector:  tableSelector("test"),
			}, false},
		{"WithCond", "SELECT * FROM test WHERE age = 10",
			selectStmt{
				tableSelector: tableSelector("test"),
				whereExpr:     Eq(FieldSelector("age"), Int64Value(10)),
			}, false},
		{"WithLimit", "SELECT * FROM test WHERE age = 10 LIMIT 20",
			selectStmt{
				tableSelector: tableSelector("test"),
				whereExpr:     Eq(FieldSelector("age"), Int64Value(10)),
				limitExpr:     Int64Value(20),
			}, false},
		{"WithOffset", "SELECT * FROM test WHERE age = 10 OFFSET 20",
			selectStmt{
				tableSelector: tableSelector("test"),
				whereExpr:     Eq(FieldSelector("age"), Int64Value(10)),
				offsetExpr:    Int64Value(20),
			}, false},
		{"WithLimitThenOffset", "SELECT * FROM test WHERE age = 10 LIMIT 10 OFFSET 20",
			selectStmt{
				tableSelector: tableSelector("test"),
				whereExpr:     Eq(FieldSelector("age"), Int64Value(10)),
				offsetExpr:    Int64Value(20),
				limitExpr:     Int64Value(10),
			}, false},
		{"WithOffsetThenLimit", "SELECT * FROM test WHERE age = 10 OFFSET 20 LIMIT 10", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := ParseQuery(test.s)
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
