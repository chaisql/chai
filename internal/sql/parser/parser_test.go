package parser_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/testutil/assert"
)

func TestParserMultiStatement(t *testing.T) {
	slct := statement.NewSelectStatement()
	slct.CompoundSelect = []*statement.SelectCoreStmt{
		{TableName: "foo", ProjectionExprs: []expr.Expr{expr.Wildcard{}}},
	}

	dlt := statement.NewDeleteStatement()
	dlt.TableName = "foo"

	tests := []struct {
		name     string
		s        string
		expected []statement.Statement
	}{
		{"OnlyCommas", ";;;", nil},
		{"TrailingComma", "SELECT * FROM foo;;;DELETE FROM foo;", []statement.Statement{
			slct,
			dlt,
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parser.ParseQuery(test.s)
			assert.NoError(t, err)
			require.EqualValues(t, test.expected, q.Statements)
		})
	}
}

func TestParserDivideByZero(t *testing.T) {
	// See https://github.com/chaisql/chai/issues/268
	require.NotPanics(t, func() {
		_, _ = parser.ParseQuery("SELECT * FROM t LIMIT 0 % .5")
	})
}
