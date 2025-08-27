package expr_test

import (
	"testing"

	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/types"
)

func TestConcatExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   types.Value
		fails bool
	}{
		{"'a' || 'b'", types.NewTextValue("ab"), false},
		{"'a' || NULL", nullLiteral, false},
		{"'a' || notFound", nullLiteral, true},
		{"'a' || 1", nullLiteral, false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testutil.TestExpr(t, test.expr, envWithRow, test.res, test.fails)
		})
	}
}
