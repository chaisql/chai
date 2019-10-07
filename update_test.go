package genji

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParserUdpate(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected Statement
		errored  bool
	}{
		{"No cond", "UPDATE test SET a = 1",
			updateStmt{
				tableName: "test",
				pairs: map[string]Expr{
					"a": Int64Value(1),
				},
			},
			false},
		{"With cond", "UPDATE test SET a = 1, b = 2 WHERE age = 10",
			updateStmt{
				tableName: "test",
				pairs: map[string]Expr{
					"a": Int64Value(1),
					"b": Int64Value(2),
				},
				whereExpr: Eq(FieldSelector("age"), Int64Value(10)),
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
