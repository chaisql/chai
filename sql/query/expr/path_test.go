package expr_test

import (
	"encoding/json"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/stretchr/testify/require"
)

func TestPathExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   document.Value
		fails bool
	}{
		{"a", document.NewIntegerValue(1), false},
		{"b", func() document.Value {
			fb := document.NewFieldBuffer()
			err := json.Unmarshal([]byte(`{"foo bar": [1, 2]}`), fb)
			require.NoError(t, err)
			return document.NewDocumentValue(fb)
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
