package parser_test

import (
	"testing"

	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/testutil/assert"
	"github.com/stretchr/testify/require"
)

func TestParserExplain(t *testing.T) {
	slct := statement.NewSelectStatement()
	slct.CompoundSelect = []*statement.SelectCoreStmt{
		{TableName: "test", ProjectionExprs: []expr.Expr{expr.Wildcard{}}},
	}

	tests := []struct {
		name     string
		s        string
		expected statement.Statement
		errored  bool
	}{
		{"Explain select", "EXPLAIN SELECT * FROM test", &statement.ExplainStmt{Statement: slct}, false},
		{"Multiple Explains", "EXPLAIN EXPLAIN CREATE TABLE test", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parser.ParseQuery(test.s)
			if test.errored {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			require.Len(t, q.Statements, 1)
			require.EqualValues(t, test.expected, q.Statements[0])
		})
	}
}
