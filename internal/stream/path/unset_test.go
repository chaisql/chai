package path_test

import (
	"testing"

	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/stream/path"
	"github.com/genjidb/genji/internal/stream/rows"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func TestUnset(t *testing.T) {
	tests := []struct {
		path  string
		in    []expr.Expr
		out   []types.Object
		fails bool
	}{
		{
			"a",
			testutil.ParseExprs(t, `{"a": 10, "b": 20}`),
			testutil.MakeObjects(t, `{"b": 20}`),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			s := stream.New(rows.Emit(test.in...)).Pipe(path.Unset(test.path))
			i := 0
			err := s.Iterate(new(environment.Environment), func(out *environment.Environment) error {
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
		require.Equal(t, path.Unset("a").String(), "paths.Unset(a)")
	})
}
