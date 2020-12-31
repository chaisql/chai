package expr_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
)

func TestArithmeticExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   document.Value
		fails bool
	}{
		{"1 + a", document.NewIntegerValue(2), false},
		{"1 + NULL", nullLitteral, false},
		{"1 + notFound", nullLitteral, false},
		{"1 - a", document.NewIntegerValue(0), false},
		{"1 - NULL", nullLitteral, false},
		{"1 - notFound", nullLitteral, false},
		{"1 * a", document.NewIntegerValue(1), false},
		{"1 * NULL", nullLitteral, false},
		{"1 * notFound", nullLitteral, false},
		{"1 / a", document.NewIntegerValue(1), false},
		{"1 / NULL", nullLitteral, false},
		{"1 / notFound", nullLitteral, false},
		{"1 % a", document.NewIntegerValue(0), false},
		{"1 % NULL", nullLitteral, false},
		{"1 % notFound", nullLitteral, false},
		{"1 & a", document.NewIntegerValue(1), false},
		{"1 & NULL", nullLitteral, false},
		{"1 & notFound", nullLitteral, false},
		{"1 | a", document.NewIntegerValue(1), false},
		{"1 | NULL", nullLitteral, false},
		{"1 | notFound", nullLitteral, false},
		{"1 ^ a", document.NewIntegerValue(0), false},
		{"1 ^ NULL", nullLitteral, false},
		{"1 ^ notFound", nullLitteral, false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testExpr(t, test.expr, envWithDoc, test.res, test.fails)
		})
	}
}

func TestArithmeticExprNodocument(t *testing.T) {
	tests := []struct {
		expr  string
		res   document.Value
		fails bool
	}{
		{"1 + a", nullLitteral, true},
		{"1 - a", nullLitteral, true},
		{"1 * a", nullLitteral, true},
		{"1 / a", nullLitteral, true},
		{"1 % a", nullLitteral, true},
		{"1 & a", nullLitteral, true},
		{"1 | a", nullLitteral, true},
		{"1 ^ a", nullLitteral, true},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.expr, func(t *testing.T) {
					emptyenv := expr.NewEnvironment(nil)

					testExpr(t, test.expr, emptyenv, test.res, test.fails)
				})
			}
		})
	}
}
