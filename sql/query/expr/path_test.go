package expr_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
)

func TestPathExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   document.Value
		fails bool
	}{
		{"a", document.NewIntegerValue(1), false},
		{"b", func() document.Value {
			d := document.NewFromJSON([]byte(`{"foo bar": [1, 2]}`))
			return document.NewDocumentValue(d)
		}(),
			false},
		{"b.`foo bar`[0]", document.NewIntegerValue(1), false},
		{"b.`foo bar`[1]", document.NewIntegerValue(2), false},
		{"b.`foo bar`[2]", nullLitteral, false},
		{"b[0]", nullLitteral, false},
		{"c[0]", document.NewIntegerValue(1), false},
		{"c[1].foo", document.NewTextValue("bar"), false},
		{"c.foo", nullLitteral, false},
		{"d", nullLitteral, false},
	}

	d := document.NewFromJSON([]byte(`{
		"a": 1,
		"b": {"foo bar": [1, 2]},
		"c": [1, {"foo": "bar"}, [1, 2]]
	}`))

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testExpr(t, test.expr, expr.EvalStack{Document: d}, test.res, test.fails)
		})
	}

	t.Run("empty stack", func(t *testing.T) {
		testExpr(t, "a", expr.EvalStack{}, nullLitteral, true)
	})
}
