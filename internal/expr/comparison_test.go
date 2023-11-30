package expr_test

import (
	"testing"

	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/types"
)

func TestComparisonExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   types.Value
		fails bool
	}{
		{"1 = a", types.NewBoolValue(true), false},
		{"1 = NULL", nullLiteral, false},
		{"1 = notFound", nullLiteral, false},
		{"1 != a", types.NewBoolValue(false), false},
		{"1 != NULL", nullLiteral, false},
		{"1 != notFound", nullLiteral, false},
		{"1 > a", types.NewBoolValue(false), false},
		{"1 > NULL", nullLiteral, false},
		{"1 > notFound", nullLiteral, false},
		{"1 >= a", types.NewBoolValue(true), false},
		{"1 >= NULL", nullLiteral, false},
		{"1 >= notFound", nullLiteral, false},
		{"1 < a", types.NewBoolValue(false), false},
		{"1 < NULL", nullLiteral, false},
		{"1 < notFound", nullLiteral, false},
		{"1 <= a", types.NewBoolValue(true), false},
		{"1 <= NULL", nullLiteral, false},
		{"1 <= notFound", nullLiteral, false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testutil.TestExpr(t, test.expr, envWithDoc, test.res, test.fails)
		})
	}
}

func TestComparisonINExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   types.Value
		fails bool
	}{
		{"1 IN []", types.NewBoolValue(false), false},
		{"1 IN [1, 2, 3]", types.NewBoolValue(true), false},
		{"2 IN [2.1, 2.2, 2.0]", types.NewBoolValue(true), false},
		{"1 IN [2, 3]", types.NewBoolValue(false), false},
		{"[1] IN [1, 2, 3]", types.NewBoolValue(false), false},
		{"[1] IN [[1], [2], [3]]", types.NewBoolValue(true), false},
		{"1 IN {}", types.NewBoolValue(false), false},
		{"[1, 2] IN 1", types.NewBoolValue(false), false},
		{"1 IN NULL", nullLiteral, false},
		{"NULL IN [1, 2, NULL]", nullLiteral, false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testutil.TestExpr(t, test.expr, envWithDoc, test.res, test.fails)
		})
	}
}

func TestComparisonNOTINExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   types.Value
		fails bool
	}{
		{"1 NOT IN []", types.NewBoolValue(true), false},
		{"1 NOT IN [1, 2, 3]", types.NewBoolValue(false), false},
		{"1 NOT IN [2, 3]", types.NewBoolValue(true), false},
		{"[1] NOT IN [1, 2, 3]", types.NewBoolValue(true), false},
		{"[1] NOT IN [[1], [2], [3]]", types.NewBoolValue(false), false},
		{"1 NOT IN {}", types.NewBoolValue(true), false},
		{"[1, 2] NOT IN 1", types.NewBoolValue(true), false},
		{"1 NOT IN NULL", nullLiteral, false},
		{"NULL NOT IN [1, 2, NULL]", nullLiteral, false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testutil.TestExpr(t, test.expr, envWithDoc, test.res, test.fails)
		})
	}
}

func TestComparisonISExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   types.Value
		fails bool
	}{
		{"1 IS 1", types.NewBoolValue(true), false},
		{"1 IS 2", types.NewBoolValue(false), false},
		{"1 IS NULL", types.NewBoolValue(false), false},
		{"NULL IS NULL", types.NewBoolValue(true), false},
		{"NULL IS 1", types.NewBoolValue(false), false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testutil.TestExpr(t, test.expr, envWithDoc, test.res, test.fails)
		})
	}
}

func TestComparisonISNOTExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   types.Value
		fails bool
	}{
		{"1 IS NOT 1", types.NewBoolValue(false), false},
		{"1 IS NOT 2", types.NewBoolValue(true), false},
		{"1 IS NOT NULL", types.NewBoolValue(true), false},
		{"NULL IS NOT NULL", types.NewBoolValue(false), false},
		{"NULL IS NOT 1", types.NewBoolValue(true), false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testutil.TestExpr(t, test.expr, envWithDoc, test.res, test.fails)
		})
	}
}

func TestComparisonExprNoObject(t *testing.T) {
	tests := []struct {
		expr  string
		res   types.Value
		fails bool
	}{
		{"1 = a", nullLiteral, true},
		{"1 != a", nullLiteral, true},
		{"1 > a", nullLiteral, true},
		{"1 >= a", nullLiteral, true},
		{"1 < a", nullLiteral, true},
		{"1 <= a", nullLiteral, true},
		{"1 IN [a]", nullLiteral, true},
		{"1 IS a", nullLiteral, true},
		{"1 IS NOT a", nullLiteral, true},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.expr, func(t *testing.T) {
					var emptyenv environment.Environment

					testutil.TestExpr(t, test.expr, &emptyenv, test.res, test.fails)
				})
			}
		})
	}
}

func TestComparisonBetweenExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   types.Value
		fails bool
	}{
		{"1 BETWEEN 0 AND 2", types.NewBoolValue(true), false},
		{"1 BETWEEN 0 AND 1", types.NewBoolValue(true), false},
		{"1 BETWEEN 1 AND 2", types.NewBoolValue(true), false},
		{"1 BETWEEN NULL AND 2", types.NewNullValue(), false},
		{"1 BETWEEN 0 AND 'foo'", types.NewBoolValue(false), false},
		{"1 BETWEEN 'foo' AND 2", types.NewBoolValue(false), false},
		{"1 BETWEEN '1' AND 2", types.NewBoolValue(false), false},
		{"1 BETWEEN CAST('1' AS int) AND 2", types.NewBoolValue(true), false},
		{"1 BETWEEN CAST('1' AS double) AND 2", types.NewBoolValue(true), false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testutil.TestExpr(t, test.expr, envWithDoc, test.res, test.fails)
		})
	}
}
