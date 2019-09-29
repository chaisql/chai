package parser

import (
	"testing"

	"github.com/asdine/genji/query"
	"github.com/asdine/genji/query/expr"
	"github.com/asdine/genji/query/q"
	"github.com/stretchr/testify/require"
)

func TestParserSelect(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected query.Statement
	}{
		{"NoCond", "SELECT * FROM test", query.Select().From(q.Table("test"))},
		{"WithFields", "SELECT a, b FROM test", query.Select(q.Field("a"), q.Field("b")).From(q.Table("test"))},
		{"WithCond", "SELECT * FROM test WHERE age = 10", query.Select().From(q.Table("test")).Where(expr.Eq(q.Field("age"), expr.Int64Value(10)))},
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
