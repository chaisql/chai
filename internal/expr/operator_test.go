package expr_test

import (
	"path/filepath"
	"testing"

	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/types"
)

func TestConcatExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   types.Value
		fails bool
	}{
		{"'a' || 'b'", types.NewTextValue("ab"), false},
		{"'a' || NULL", nullLiteral, false},
		{"'a' || notFound", nullLiteral, false},
		{"'a' || 1", nullLiteral, false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testutil.TestExpr(t, test.expr, envWithDoc, test.res, test.fails)
		})
	}
}

func TestCast(t *testing.T) {
	testutil.ExprRunner(t, filepath.Join("testdata", "cast.sql"))
}
