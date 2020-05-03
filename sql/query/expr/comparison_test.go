package expr_test

import (
	"testing"

	"github.com/asdine/genji/document"
	"github.com/asdine/genji/sql/query/expr"
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
			testExpr(t, test.expr, stackWithDoc, test.res, test.fails)
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
			testExpr(t, test.expr, stackWithDoc, test.res, test.fails)
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
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.expr, func(t *testing.T) {
					var emptyStack expr.EvalStack

					testExpr(t, test.expr, emptyStack, test.res, test.fails)
				})
			}
		})
	}
}
