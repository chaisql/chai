package stream_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/stream"
	"github.com/genjidb/genji/sql/parser"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/stretchr/testify/require"
)

func TestProject(t *testing.T) {
	tests := []struct {
		name  string
		exprs []expr.Expr
		in    document.Value
		out   string
		fails bool
	}{
		{
			"Value/Constant",
			[]expr.Expr{parser.MustParseExpr("10")},
			document.NewIntegerValue(1),
			`{"_v":{"10":10}}`,
			false,
		},
		{
			"Value/Wildcard",
			[]expr.Expr{expr.Wildcard{}},
			document.NewIntegerValue(1),
			`{"_v":{"1":1}}`,
			false,
		},
		{
			"Value/Multiple",
			[]expr.Expr{expr.Wildcard{}, expr.Wildcard{}, parser.MustParseExpr("10")},
			document.NewIntegerValue(1),
			`{"_v":{"1":1,"1":1,"10":10}}`,
			false,
		},
		{
			"Document/Constant",
			[]expr.Expr{parser.MustParseExpr("10")},
			document.NewDocumentValue(document.NewFromJSON([]byte(`{"a":1,"b":[true]}`))),
			`{"_v":{"10":10}}`,
			false,
		},
		{
			"Document/Wildcard",
			[]expr.Expr{expr.Wildcard{}},
			document.NewDocumentValue(document.NewFromJSON([]byte(`{"a":1,"b":[true]}`))),
			`{"_v":{"a":1,"b":[true]}}`,
			false,
		},
		{
			"Document/Multiple",
			[]expr.Expr{expr.Wildcard{}, expr.Wildcard{}, parser.MustParseExpr("10")},
			document.NewDocumentValue(document.NewFromJSON([]byte(`{"a":1,"b":[true]}`))),
			`{"_v":{"a":1,"b":[true],"a":1,"b":[true],"10":10}}`,
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s", test.name), func(t *testing.T) {
			var inEnv expr.Environment
			inEnv.SetCurrentValue(test.in)

			env, err := stream.Project(test.exprs...).Op()(&inEnv)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, &inEnv, env.Outer)
				got, _ := json.Marshal(env.Buf)
				require.Equal(t, test.out, string(got))
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, "project(1, *, *, 1 + 1)", stream.Project(
			parser.MustParseExpr("1"),
			expr.Wildcard{},
			expr.Wildcard{},
			parser.MustParseExpr("1 +    1"),
		).String())
	})
}
