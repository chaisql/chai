package expr_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
)

func TestPkExpr(t *testing.T) {
	tests := []struct {
		name  string
		stack expr.EvalStack
		res   document.Value
		fails bool
	}{
		{"empty stack", expr.EvalStack{}, nullLitteral, true},
		{"stack with doc", stackWithDoc, nullLitteral, true},
		{"stack with doc and info", stackWithDocAndInfo, document.NewIntValue(1), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testExpr(t, "pk()", test.stack, test.res, test.fails)
		})
	}
}
