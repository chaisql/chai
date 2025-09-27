package parser_test

import (
	"testing"

	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/stretchr/testify/require"
)

// Basic tests for the short cast form (::) to ensure it parses into the same
// Cast AST node as CAST(... AS ...), respects precedence, and supports chaining.
func TestParserCastShortForm(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{"simple literal cast", "SELECT '1'::INTEGER", "CAST('1' AS integer)"},
		{"cast sum with parens", "SELECT (1 + 2)::DOUBLE PRECISION", "CAST((1 + 2) AS double precision)"},
		{"cast binds tighter than +", "SELECT 1 + 2::DOUBLE PRECISION", "1 + CAST(2 AS double precision)"},
		{"chained casts", "SELECT a::INT::BIGINT FROM foo", "CAST(CAST(a AS integer) AS bigint)"},
		{"unary minus then cast", "SELECT -1::INT", "CAST(-1 AS integer)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.ParseQuery(tt.sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)

			// Expect a SELECT statement
			slct, ok := stmts[0].(*statement.SelectStmt)
			require.True(t, ok)
			require.NotEmpty(t, slct.CompoundSelect)

			// Look at first projection expression's string representation.
			proj := slct.CompoundSelect[0].ProjectionExprs[0]

			// For readability, use expr.String() via fmt.Sprintf("%v") which is
			// what the Expr implementations print via their String() methods.
			got := exprToString(proj)

			// Some cases (like the plain addition with cast) will produce parentheses
			// around the expression. Normalize expected strings accordingly by
			// matching substring where helpful. For strictness, compare the exact
			// string when provided.
			require.Equal(t, tt.want, got)
		})
	}
}

// exprToString returns the string representation of an expr.Expr; keep a tiny
// helper to centralize formatting in case of future changes.
func exprToString(e expr.Expr) string {
	return e.String()
}
