package expr_test

import (
	"testing"

	"github.com/genjidb/genji/document"
)

func TestConcatExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   document.Value
		fails bool
	}{
		{"'a' || 'b'", document.NewTextValue("ab"), false},
		{"'a' || NULL", nullLitteral, false},
		{"'a' || notFound", nullLitteral, false},
		{"'a' || 1", nullLitteral, false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testExpr(t, test.expr, envWithDoc, test.res, test.fails)
		})
	}
}
