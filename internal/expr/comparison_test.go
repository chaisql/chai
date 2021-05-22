package expr_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/expr"
)

func TestComparisonExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   document.Value
		fails bool
	}{
		{"1 = a", document.NewBoolValue(true), false},
		{"1 = NULL", nullLitteral, false},
		{"1 = notFound", nullLitteral, false},
		{"1 != a", document.NewBoolValue(false), false},
		{"1 != NULL", nullLitteral, false},
		{"1 != notFound", nullLitteral, false},
		{"1 > a", document.NewBoolValue(false), false},
		{"1 > NULL", nullLitteral, false},
		{"1 > notFound", nullLitteral, false},
		{"1 >= a", document.NewBoolValue(true), false},
		{"1 >= NULL", nullLitteral, false},
		{"1 >= notFound", nullLitteral, false},
		{"1 < a", document.NewBoolValue(false), false},
		{"1 < NULL", nullLitteral, false},
		{"1 < notFound", nullLitteral, false},
		{"1 <= a", document.NewBoolValue(true), false},
		{"1 <= NULL", nullLitteral, false},
		{"1 <= notFound", nullLitteral, false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testExpr(t, test.expr, envWithDoc, test.res, test.fails)
		})
	}
}

func TestComparisonINExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   document.Value
		fails bool
	}{
		{"1 IN []", document.NewBoolValue(false), false},
		{"1 IN [1, 2, 3]", document.NewBoolValue(true), false},
		{"2 IN [2.1, 2.2, 2.0]", document.NewBoolValue(true), false},
		{"1 IN [2, 3]", document.NewBoolValue(false), false},
		{"[1] IN [1, 2, 3]", document.NewBoolValue(false), false},
		{"[1] IN [[1], [2], [3]]", document.NewBoolValue(true), false},
		{"1 IN {}", document.NewBoolValue(false), false},
		{"[1, 2] IN 1", document.NewBoolValue(false), false},
		{"1 IN NULL", nullLitteral, false},
		{"NULL IN [1, 2, NULL]", nullLitteral, false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testExpr(t, test.expr, envWithDoc, test.res, test.fails)
		})
	}
}

func TestComparisonNOTINExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   document.Value
		fails bool
	}{
		{"1 NOT IN []", document.NewBoolValue(true), false},
		{"1 NOT IN [1, 2, 3]", document.NewBoolValue(false), false},
		{"1 NOT IN [2, 3]", document.NewBoolValue(true), false},
		{"[1] NOT IN [1, 2, 3]", document.NewBoolValue(true), false},
		{"[1] NOT IN [[1], [2], [3]]", document.NewBoolValue(false), false},
		{"1 NOT IN {}", document.NewBoolValue(true), false},
		{"[1, 2] NOT IN 1", document.NewBoolValue(true), false},
		{"1 NOT IN NULL", nullLitteral, false},
		{"NULL NOT IN [1, 2, NULL]", nullLitteral, false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testExpr(t, test.expr, envWithDoc, test.res, test.fails)
		})
	}
}

func TestComparisonISExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   document.Value
		fails bool
	}{
		{"1 IS 1", document.NewBoolValue(true), false},
		{"1 IS 2", document.NewBoolValue(false), false},
		{"1 IS NULL", document.NewBoolValue(false), false},
		{"NULL IS NULL", document.NewBoolValue(true), false},
		{"NULL IS 1", document.NewBoolValue(false), false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testExpr(t, test.expr, envWithDoc, test.res, test.fails)
		})
	}
}

func TestComparisonISNOTExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   document.Value
		fails bool
	}{
		{"1 IS NOT 1", document.NewBoolValue(false), false},
		{"1 IS NOT 2", document.NewBoolValue(true), false},
		{"1 IS NOT NULL", document.NewBoolValue(true), false},
		{"NULL IS NOT NULL", document.NewBoolValue(false), false},
		{"NULL IS NOT 1", document.NewBoolValue(true), false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testExpr(t, test.expr, envWithDoc, test.res, test.fails)
		})
	}
}

func TestComparisonExprNodocument(t *testing.T) {
	tests := []struct {
		expr  string
		res   document.Value
		fails bool
	}{
		{"1 = a", nullLitteral, true},
		{"1 != a", nullLitteral, true},
		{"1 > a", nullLitteral, true},
		{"1 >= a", nullLitteral, true},
		{"1 < a", nullLitteral, true},
		{"1 <= a", nullLitteral, true},
		{"1 IN [a]", nullLitteral, true},
		{"1 IS a", nullLitteral, true},
		{"1 IS NOT a", nullLitteral, true},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.expr, func(t *testing.T) {
					var emptyenv expr.Environment

					testExpr(t, test.expr, &emptyenv, test.res, test.fails)
				})
			}
		})
	}
}

func TestComparisonBetweenExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   document.Value
		fails bool
	}{
		{"1 BETWEEN 0 AND 2", document.NewBoolValue(true), false},
		{"1 BETWEEN 0 AND 1", document.NewBoolValue(true), false},
		{"1 BETWEEN 1 AND 2", document.NewBoolValue(true), false},
		{"1 BETWEEN NULL AND 2", document.NewNullValue(), false},
		{"1 BETWEEN 0 AND 'foo'", document.NewBoolValue(false), false},
		{"1 BETWEEN 'foo' AND 2", document.NewBoolValue(false), false},
		{"1 BETWEEN '1' AND 2", document.NewBoolValue(false), false},
		{"1 BETWEEN CAST('1' AS int) AND 2", document.NewBoolValue(true), false},
		{"1 BETWEEN CAST('1' AS double) AND 2", document.NewBoolValue(true), false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testExpr(t, test.expr, envWithDoc, test.res, test.fails)
		})
	}
}
