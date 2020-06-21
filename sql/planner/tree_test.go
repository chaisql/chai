package planner

import (
	"testing"

	"github.com/genjidb/genji/sql/query/expr"
)

func TestNodeEqual(t *testing.T) {
	tests := []struct {
		name  string
		a     *node
		b     Node
		equal bool
	}{
		{"two nils", (*node)(nil), nil, true},
		{"a nil", (*node)(nil), NewSelectionNode(nil, expr.BoolValue(true)), false},
		{"b nil", &node{op: Selection}, nil, false},
		{"b nil", &node{op: Selection}, NewSelectionNode(nil, nil), true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.a.IsEqual(test.b)
		})
	}
}
