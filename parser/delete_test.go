package parser

import (
	"testing"

	"github.com/asdine/genji/query"
	"github.com/stretchr/testify/require"
)

func TestParserDelete(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected query.Statement
	}{
		{"NoCond", "DELETE FROM test", query.DeleteStmt{TableName: "test"}},
		{"WithCond", "DELETE FROM test WHERE age = 10", query.DeleteStmt{TableName: "test", WhereExpr: query.Eq(query.FieldSelector("age"), query.Int8Value(10))}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := ParseQuery(test.s)
			require.NoError(t, err)
			require.Len(t, q.Statements, 1)
			require.EqualValues(t, test.expected, q.Statements[0])
		})
	}
}
