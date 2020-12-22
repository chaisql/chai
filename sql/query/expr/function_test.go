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
	}{
		{"empty stack", expr.EvalStack{}, nullLitteral},
		{"stack with doc", stackWithDoc, nullLitteral},
		{"stack with doc and key", stackWithDocAndKey, document.NewIntegerValue(1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testExpr(t, "pk()", test.stack, test.res, false)
		})
	}
}
