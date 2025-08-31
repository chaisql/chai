package parser_test

import (
	"testing"

	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/stretchr/testify/require"
)

func TestParserExplain(t *testing.T) {
	var slct statement.SelectStmt
	slct.CompoundSelect = []*statement.SelectCoreStmt{
		{TableName: "test", ProjectionExprs: []expr.Expr{expr.Wildcard{}}},
	}

	tests := []struct {
		name     string
		s        string
		expected statement.Statement
		errored  bool
	}{
		{"Explain select", "EXPLAIN SELECT * FROM test", &statement.ExplainStmt{Statement: &slct}, false},
		{"Multiple Explains", "EXPLAIN EXPLAIN CREATE TABLE test", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmts, err := parser.ParseQuery(test.s)
			if test.errored {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			require.EqualValues(t, test.expected, stmts[0])
		})
	}
}
