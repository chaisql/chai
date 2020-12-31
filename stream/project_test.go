package stream_test

import (
	"fmt"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/parser"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/genjidb/genji/stream"
	"github.com/stretchr/testify/require"
)

func TestProject(t *testing.T) {
	tests := []struct {
		name  string
		exprs []expr.Expr
		in    document.Document
		out   string
		fails bool
	}{
		{
			"Constant",
			[]expr.Expr{parser.MustParseExpr("10")},
			docFromJSON(`{"a":1,"b":[true]}`),
			`{"10":10}`,
			false,
		},
		{
			"Wildcard",
			[]expr.Expr{expr.Wildcard{}},
			docFromJSON(`{"a":1,"b":[true]}`),
			`{"a":1,"b":[true]}`,
			false,
		},
		{
			"Multiple",
			[]expr.Expr{expr.Wildcard{}, expr.Wildcard{}, parser.MustParseExpr("10")},
			docFromJSON(`{"a":1,"b":[true]}`),
			`{"a":1,"b":[true],"a":1,"b":[true],"10":10}`,
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s", test.name), func(t *testing.T) {
			var inEnv expr.Environment
			inEnv.SetDocument(test.in)

			op, err := stream.Project(test.exprs...).Op(stream.Stream{})
			require.NoError(t, err)
			env, err := op(&inEnv)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, &inEnv, env.Outer)
				d, ok := env.GetDocument()
				require.True(t, ok)
				require.JSONEq(t, test.out, document.NewDocumentValue(d).String())

				err = d.Iterate(func(field string, want document.Value) error {
					got, err := d.GetByField(field)
					require.NoError(t, err)
					require.Equal(t, want, got)
					return nil
				})
				require.NoError(t, err)
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
