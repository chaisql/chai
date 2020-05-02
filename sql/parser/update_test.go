package parser

import (
	"testing"

	"github.com/asdine/genji/sql/query"
	"github.com/asdine/genji/sql/query/expr"
	"github.com/stretchr/testify/require"
)

func TestParserUdpate(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected query.Statement
		errored  bool
	}{
		{"No cond", "UPDATE test SET a = 1",
			query.UpdateStmt{
				TableName: "test",
				Pairs: map[string]expr.Expr{
					"a": expr.IntValue(1),
				},
			},
			false},
		{"With cond", "UPDATE test SET a = 1, b = 2 WHERE age = 10",
			query.UpdateStmt{
				TableName: "test",
				Pairs: map[string]expr.Expr{
					"a": expr.IntValue(1),
					"b": expr.IntValue(2),
				},
				WhereExpr: expr.Eq(expr.FieldSelector([]string{"age"}), expr.IntValue(10)),
			},
			false},
		{"Trailing comma", "UPDATE test SET a = 1, WHERE age = 10", nil, true},
		{"No SET", "UPDATE test WHERE age = 10", nil, true},
		{"No pair", "UPDATE test SET WHERE age = 10", nil, true},
		{"query.Field only", "UPDATE test SET a WHERE age = 10", nil, true},
		{"No value", "UPDATE test SET a = WHERE age = 10", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := ParseQuery(test.s)
			if test.errored {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, q.Statements, 1)
			require.EqualValues(t, test.expected, q.Statements[0])
		})
	}
}
