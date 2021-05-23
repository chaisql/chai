package stream_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/testutil"
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
			testutil.MakeDocument(t, `{"a":1,"b":[true]}`),
			`{"10":10}`,
			false,
		},
		{
			"Wildcard",
			[]expr.Expr{expr.Wildcard{}},
			testutil.MakeDocument(t, `{"a":1,"b":[true]}`),
			`{"a":1,"b":[true]}`,
			false,
		},
		{
			"Multiple",
			[]expr.Expr{expr.Wildcard{}, expr.Wildcard{}, parser.MustParseExpr("10")},
			testutil.MakeDocument(t, `{"a":1,"b":[true]}`),
			`{"a":1,"b":[true],"a":1,"b":[true],"10":10}`,
			false,
		},
		{
			"Named",
			[]expr.Expr{&expr.NamedExpr{Expr: parser.MustParseExpr("10"), ExprName: "foo"}},
			testutil.MakeDocument(t, `{"a":1,"b":[true]}`),
			`{"foo":10}`,
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var inEnv expr.Environment
			inEnv.SetDocument(test.in)

			err := stream.Project(test.exprs...).Iterate(&inEnv, func(out *expr.Environment) error {
				require.Equal(t, &inEnv, out.Outer)
				d, ok := out.GetDocument()
				require.True(t, ok)
				require.JSONEq(t, test.out, document.NewDocumentValue(d).String())

				err := d.Iterate(func(field string, want document.Value) error {
					got, err := d.GetByField(field)
					require.NoError(t, err)
					require.Equal(t, want, got)
					return nil
				})
				require.NoError(t, err)
				return nil
			})
			if test.fails {
				require.Error(t, err)
			} else {
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

	t.Run("No input", func(t *testing.T) {
		stream.Project(parser.MustParseExpr("1 + 1")).Iterate(expr.NewEnvironment(nil), func(out *expr.Environment) error {
			d, ok := out.GetDocument()
			require.True(t, ok)
			enc, err := document.MarshalJSON(d)
			require.NoError(t, err)
			require.JSONEq(t, `{"1 + 1": 2}`, string(enc))
			return nil
		})
	})
}
