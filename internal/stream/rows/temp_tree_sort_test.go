package rows_test

import (
	"testing"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/rows"
	"github.com/chaisql/chai/internal/stream/table"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestTempTreeSort(t *testing.T) {
	tests := []struct {
		name     string
		sortExpr expr.Expr
		values   []any
		want     []row.Row
		fails    bool
		desc     bool
	}{
		{
			"ASC",
			parser.MustParseExpr("a"),
			[]any{0, nil, true},
			[]row.Row{
				testutil.MakeRow(t, `{"a": null}`),
				testutil.MakeRow(t, `{"a": 0}`),
				testutil.MakeRow(t, `{"a": 1}`),
			},
			false,
			false,
		},
		{
			"DESC",
			parser.MustParseExpr("a"),
			[]any{0, nil, true},
			[]row.Row{
				testutil.MakeRow(t, `{"a": 1}`),
				testutil.MakeRow(t, `{"a": 0}`),
				testutil.MakeRow(t, `{"a": null}`),
			},
			false,
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, "CREATE TABLE test(pk int primary key, a int)")

			for i, val := range test.values {
				testutil.MustExec(t, db, tx, "INSERT INTO test (pk, a) VALUES ($1, $2)", environment.Param{Value: i + 1}, environment.Param{Value: val})
			}

			env := environment.New(db, tx, nil, nil)

			s := stream.New(table.Scan("test")).Pipe(rows.Project(parser.MustParseExpr("a")))
			if test.desc {
				s = s.Pipe(rows.TempTreeSortReverse(test.sortExpr))
			} else {
				s = s.Pipe(rows.TempTreeSort(test.sortExpr))
			}

			var got []row.Row
			err := s.Iterate(env, func(r database.Row) error {
				fb := row.NewColumnBuffer()
				err := fb.Copy(r)
				if err != nil {
					return err
				}
				got = append(got, fb)
				return nil
			})

			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, len(got), len(test.want))
				for i := range got {
					testutil.RequireRowEqual(t, test.want[i], got[i])
				}
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, `rows.TempTreeSort(a)`, rows.TempTreeSort(parser.MustParseExpr("a")).String())
	})
}
