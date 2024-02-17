package expr_test

import (
	"testing"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/types"
)

var envWithRow = environment.New(func() database.Row {
	return database.NewBasicRow(row.NewColumnBuffer().Add("a", types.NewIntegerValue(1)))
}())

func TestComparisonExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   types.Value
		fails bool
	}{
		{"1 = a", types.NewBooleanValue(true), false},
		{"1 = NULL", nullLiteral, false},
		{"1 = notFound", nullLiteral, true},
		{"1 != a", types.NewBooleanValue(false), false},
		{"1 != NULL", nullLiteral, false},
		{"1 != notFound", nullLiteral, true},
		{"1 > a", types.NewBooleanValue(false), false},
		{"1 > NULL", nullLiteral, false},
		{"1 > notFound", nullLiteral, true},
		{"1 >= a", types.NewBooleanValue(true), false},
		{"1 >= NULL", nullLiteral, false},
		{"1 >= notFound", nullLiteral, true},
		{"1 < a", types.NewBooleanValue(false), false},
		{"1 < NULL", nullLiteral, false},
		{"1 < notFound", nullLiteral, true},
		{"1 <= a", types.NewBooleanValue(true), false},
		{"1 <= NULL", nullLiteral, false},
		{"1 <= notFound", nullLiteral, true},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testutil.TestExpr(t, test.expr, envWithRow, test.res, test.fails)
		})
	}
}

func TestComparisonINExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   types.Value
		fails bool
	}{
		{"1 IN (2)", types.NewBooleanValue(false), false},
		{"1 IN (1, 2, 3)", types.NewBooleanValue(true), false},
		{"2 IN (2.1, 2.2, 2.0)", types.NewBooleanValue(true), false},
		{"1 IN (2, 3)", types.NewBooleanValue(false), false},
		{"(1) IN (1, 2, 3)", types.NewBooleanValue(true), false},
		{"(1) IN (1), (2), (3)", types.NewBooleanValue(true), false},
		{"NULL IN (1, 2, NULL)", nullLiteral, false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testutil.TestExpr(t, test.expr, envWithRow, test.res, test.fails)
		})
	}
}

func TestComparisonNOTINExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   types.Value
		fails bool
	}{
		{"1 NOT IN (1, 2, 3)", types.NewBooleanValue(false), false},
		{"1 NOT IN (2, 3)", types.NewBooleanValue(true), false},
		{"(1) NOT IN (1, 2, 3)", types.NewBooleanValue(false), false},
		{"NULL NOT IN (1, 2, NULL)", nullLiteral, false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testutil.TestExpr(t, test.expr, envWithRow, test.res, test.fails)
		})
	}
}

func TestComparisonISExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   types.Value
		fails bool
	}{
		{"1 IS 1", types.NewBooleanValue(true), false},
		{"1 IS 2", types.NewBooleanValue(false), false},
		{"1 IS NULL", types.NewBooleanValue(false), false},
		{"NULL IS NULL", types.NewBooleanValue(true), false},
		{"NULL IS 1", types.NewBooleanValue(false), false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testutil.TestExpr(t, test.expr, envWithRow, test.res, test.fails)
		})
	}
}

func TestComparisonISNOTExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   types.Value
		fails bool
	}{
		{"1 IS NOT 1", types.NewBooleanValue(false), false},
		{"1 IS NOT 2", types.NewBooleanValue(true), false},
		{"1 IS NOT NULL", types.NewBooleanValue(true), false},
		{"NULL IS NOT NULL", types.NewBooleanValue(false), false},
		{"NULL IS NOT 1", types.NewBooleanValue(true), false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testutil.TestExpr(t, test.expr, envWithRow, test.res, test.fails)
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
		{"1 IN (a)", nullLiteral, true},
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
		{"1 BETWEEN 0 AND 2", types.NewBooleanValue(true), false},
		{"1 BETWEEN 0 AND 1", types.NewBooleanValue(true), false},
		{"1 BETWEEN 1 AND 2", types.NewBooleanValue(true), false},
		{"1 BETWEEN NULL AND 2", types.NewNullValue(), false},
		{"1 BETWEEN 0 AND 'foo'", types.NewBooleanValue(false), false},
		{"1 BETWEEN 'foo' AND 2", types.NewBooleanValue(false), false},
		{"1 BETWEEN '1' AND 2", types.NewBooleanValue(false), false},
		{"1 BETWEEN CAST('1' AS int) AND 2", types.NewBooleanValue(true), false},
		{"1 BETWEEN CAST('1' AS double) AND 2", types.NewBooleanValue(true), false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testutil.TestExpr(t, test.expr, envWithRow, test.res, test.fails)
		})
	}
}
