package docs_test

import (
	"encoding/json"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/stream/docs"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func TestProject(t *testing.T) {
	tests := []struct {
		name  string
		exprs []expr.Expr
		in    types.Document
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
			var inEnv environment.Environment
			inEnv.SetDocument(test.in)

			err := docs.Project(test.exprs...).Iterate(&inEnv, func(out *environment.Environment) error {
				require.Equal(t, &inEnv, out.GetOuter())
				d, ok := out.GetDocument()
				require.True(t, ok)
				tt, err := json.Marshal(types.NewDocumentValue(d))
				require.NoError(t, err)
				require.JSONEq(t, test.out, string(tt))

				err = d.Iterate(func(field string, want types.Value) error {
					got, err := d.GetByField(field)
					assert.NoError(t, err)
					require.Equal(t, want, got)
					return nil
				})
				assert.NoError(t, err)
				return nil
			})
			if test.fails {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, "docs.Project(1, *, *, 1 + 1)", docs.Project(
			parser.MustParseExpr("1"),
			expr.Wildcard{},
			expr.Wildcard{},
			parser.MustParseExpr("1 +    1"),
		).String())
	})

	t.Run("No input", func(t *testing.T) {
		docs.Project(parser.MustParseExpr("1 + 1")).Iterate(new(environment.Environment), func(out *environment.Environment) error {
			d, ok := out.GetDocument()
			require.True(t, ok)
			enc, err := document.MarshalJSON(d)
			assert.NoError(t, err)
			require.JSONEq(t, `{"1 + 1": 2}`, string(enc))
			return nil
		})
	})
}
