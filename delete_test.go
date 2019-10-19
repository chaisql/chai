package genji

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParserDelete(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected statement
	}{
		{"NoCond", "DELETE FROM test", deleteStmt{tableName: "test"}},
		{"WithCond", "DELETE FROM test WHERE age = 10", deleteStmt{tableName: "test", whereExpr: eq(fieldSelector("age"), int64Value(10))}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parseQuery(test.s)
			require.NoError(t, err)
			require.Len(t, q.Statements, 1)
			require.EqualValues(t, test.expected, q.Statements[0])
		})
	}
}
