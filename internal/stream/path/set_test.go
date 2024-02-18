package path_test

import (
	"testing"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/path"
	"github.com/chaisql/chai/internal/stream/rows"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestSet(t *testing.T) {
	tests := []struct {
		column string
		e      expr.Expr
		in     []expr.Row
		out    []row.Row
		fails  bool
	}{
		{
			"a",
			parser.MustParseExpr(`10`),
			testutil.MakeRowExprs(t, `{"a": true}`),
			testutil.MakeRows(t, `{"a": 10}`),
			false,
		},
		{
			"b",
			parser.MustParseExpr(`10`),
			testutil.MakeRowExprs(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeRows(t, `{"a": 1, "b": 10}`, `{"a": 2, "b": 10}`),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.column, func(t *testing.T) {
			s := stream.New(rows.Emit([]string{"a"}, test.in...)).Pipe(path.Set(test.column, test.e))
			i := 0
			err := s.Iterate(new(environment.Environment), func(out *environment.Environment) error {
				r, _ := out.GetRow()
				testutil.RequireRowEqual(t, test.out[i], r)
				i++
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
		require.Equal(t, path.Set("a", parser.MustParseExpr("1")).String(), "paths.Set(a, 1)")
	})
}
