package path_test

import (
	"testing"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/path"
	"github.com/chaisql/chai/internal/stream/rows"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/testutil/assert"
	"github.com/stretchr/testify/require"
)

func TestPathsRename(t *testing.T) {
	tests := []struct {
		fieldNames []string
		in         []expr.Row
		out        []row.Row
		fails      bool
	}{
		{
			[]string{"c", "d"},
			testutil.MakeRowExprs(t, `{"a": 10, "b": 20}`),
			testutil.MakeRows(t, `{"c": 10, "d": 20}`),
			false,
		},
		{
			[]string{"c", "d", "e"},
			testutil.MakeRowExprs(t, `{"a": 10, "b": 20}`),
			nil,
			true,
		},
		{
			[]string{"c"},
			testutil.MakeRowExprs(t, `{"a": 10, "b": 20}`),
			nil,
			true,
		},
	}

	for _, test := range tests {
		s := stream.New(rows.Emit(test.in...)).Pipe(path.PathsRename(test.fieldNames...))
		t.Run(s.String(), func(t *testing.T) {
			i := 0
			err := s.Iterate(new(environment.Environment), func(out *environment.Environment) error {
				r, _ := out.GetRow()
				testutil.RequireRowEqual(t, test.out[i], r)
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
		require.Equal(t, path.PathsRename("a", "b", "c").String(), "paths.Rename(a, b, c)")
	})
}
