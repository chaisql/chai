package rows_test

import (
	"testing"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/object"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/rows"
	"github.com/chaisql/chai/internal/stream/table"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/testutil/assert"
	"github.com/chaisql/chai/internal/types"
	"github.com/stretchr/testify/require"
)

func TestTempTreeSort(t *testing.T) {
	tests := []struct {
		name     string
		sortExpr expr.Expr
		values   []types.Object
		want     []types.Object
		fails    bool
		desc     bool
	}{
		{
			"ASC",
			parser.MustParseExpr("a"),
			[]types.Object{
				testutil.MakeObject(t, `{"a": 0}`),
				testutil.MakeObject(t, `{"a": null}`),
				testutil.MakeObject(t, `{"a": true}`),
			},
			[]types.Object{
				testutil.MakeObject(t, `{}`),
				testutil.MakeObject(t, `{"a": 0}`),
				testutil.MakeObject(t, `{"a": 1}`),
			},
			false,
			false,
		},
		{
			"DESC",
			parser.MustParseExpr("a"),
			[]types.Object{
				testutil.MakeObject(t, `{"a": 0}`),
				testutil.MakeObject(t, `{"a": null}`),
				testutil.MakeObject(t, `{"a": true}`),
			},
			[]types.Object{
				testutil.MakeObject(t, `{"a": 1}`),
				testutil.MakeObject(t, `{"a": 0}`),
				testutil.MakeObject(t, `{}`),
			},
			false,
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, "CREATE TABLE test(a int)")

			for _, doc := range test.values {
				testutil.MustExec(t, db, tx, "INSERT INTO test VALUES ?", environment.Param{Value: doc})
			}

			var env environment.Environment
			env.DB = db
			env.Tx = tx

			s := stream.New(table.Scan("test"))
			if test.desc {
				s = s.Pipe(rows.TempTreeSortReverse(test.sortExpr))
			} else {
				s = s.Pipe(rows.TempTreeSort(test.sortExpr))
			}

			var got []types.Object
			err := s.Iterate(&env, func(env *environment.Environment) error {
				r, ok := env.GetRow()
				require.True(t, ok)

				fb := object.NewFieldBuffer()
				fb.Copy(r.Object())
				got = append(got, fb)
				return nil
			})

			if test.fails {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				require.Equal(t, len(got), len(test.want))
				for i := range got {
					testutil.RequireObjEqual(t, test.want[i], got[i])
				}
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, `rows.TempTreeSort(a)`, rows.TempTreeSort(parser.MustParseExpr("a")).String())
	})
}
