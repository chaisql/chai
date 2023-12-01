package path_test

import (
	"testing"

	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/object"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/stream/path"
	"github.com/genjidb/genji/internal/stream/rows"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/internal/types"
	"github.com/stretchr/testify/require"
)

func TestSet(t *testing.T) {
	tests := []struct {
		path  string
		e     expr.Expr
		in    []expr.Expr
		out   []types.Object
		fails bool
	}{
		{
			"a[0].b",
			parser.MustParseExpr(`10`),
			testutil.ParseExprs(t, `{"a": [{}]}`),
			testutil.MakeObjects(t, `{"a": [{"b": 10}]}`),
			false,
		},
		{
			"a[2]",
			parser.MustParseExpr(`10`),
			testutil.ParseExprs(t, `{"a": [1]}`, `{"a": [1, 2, 3]}`),
			testutil.MakeObjects(t, `{"a": [1, 2, 10]}`),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			p, err := parser.ParsePath(test.path)
			assert.NoError(t, err)
			s := stream.New(rows.Emit(test.in...)).Pipe(path.Set(p, test.e))
			i := 0
			err = s.Iterate(new(environment.Environment), func(out *environment.Environment) error {
				r, _ := out.GetRow()
				require.Equal(t, test.out[i], r.Object())
				i++
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
		require.Equal(t, path.Set(object.NewPath("a", "b"), parser.MustParseExpr("1")).String(), "paths.Set(a.b, 1)")
	})
}
