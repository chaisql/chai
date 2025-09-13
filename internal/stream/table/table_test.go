package table_test

import (
	"testing"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/table"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/types"
	"github.com/stretchr/testify/require"
)

func TestTableScan(t *testing.T) {
	tests := []struct {
		name                  string
		docsInTable, expected testutil.Rows
		ranges                stream.Ranges
		reverse               bool
		fails                 bool
	}{
		{
			"no-range",
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			nil,
			false,
			false,
		},
		{
			"no-range:reverse",
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeRows(t, `{"a": 2}`, `{"a": 1}`),
			nil,
			true,
			false,
		},
		{
			"max:2",
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			stream.Ranges{
				stream.Range{Max: testutil.ExprList(t, `(2)`)},
			},
			false, false,
		},
		{
			"max:1",
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeRows(t, `{"a": 1}`),
			stream.Ranges{
				stream.Range{Max: testutil.ExprList(t, `(1)`)},
			},
			false, false,
		},
		{
			"max:1.1",
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			nil,
			stream.Ranges{
				stream.Range{Max: testutil.ExprList(t, `(1.1)`)},
			},
			false, false,
		},
		{
			"min",
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			stream.Ranges{
				stream.Range{Min: testutil.ExprList(t, `(1)`)},
			},
			false, false,
		},
		{
			"min:0.5",
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			nil,
			stream.Ranges{
				stream.Range{Min: testutil.ExprList(t, `(0.5)`)},
			},
			false, false,
		},
		{
			"min/max",
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			stream.Ranges{
				stream.Range{Min: testutil.ExprList(t, `(1)`), Max: testutil.ExprList(t, `(2)`)},
			},
			false, false,
		},
		{
			"min/max:0.5/1.5",
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			nil,
			stream.Ranges{
				stream.Range{Min: testutil.ExprList(t, `(0.5)`), Max: testutil.ExprList(t, `(1.5)`)},
			},
			false, false,
		},
		{
			"reverse/max",
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeRows(t, `{"a": 2}`, `{"a": 1}`),
			stream.Ranges{
				stream.Range{Max: testutil.ExprList(t, `(2)`)},
			},
			true, false,
		},
		{
			"reverse/min",
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeRows(t, `{"a": 2}`, `{"a": 1}`),
			stream.Ranges{
				stream.Range{Min: testutil.ExprList(t, `(1)`)},
			},
			true, false,
		},
		{
			"reverse/min/max",
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeRows(t, `{"a": 2}`, `{"a": 1}`),
			stream.Ranges{
				stream.Range{Min: testutil.ExprList(t, `(1)`), Max: testutil.ExprList(t, `(2)`)},
			},
			true, false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, "CREATE TABLE test (a INTEGER NOT NULL PRIMARY KEY)")

			for _, r := range test.docsInTable {
				v, err := r.Get("a")
				require.NoError(t, err)

				testutil.MustExec(t, db, tx, "INSERT INTO test VALUES ($1)", environment.Param{Value: types.AsInt64(v)})
			}

			op := table.Scan("test", test.ranges...)
			op.Reverse = test.reverse
			env := environment.New(nil, tx, []environment.Param{{Name: "foo", Value: 1}}, nil)

			var i int
			var got testutil.Rows
			err := stream.New(op).Iterate(env, func(r database.Row) error {
				var fb row.ColumnBuffer

				err := fb.Copy(r)
				require.NoError(t, err)

				got = append(got, &fb)
				v, err := env.GetParamByIndex(1)
				require.NoError(t, err)
				require.Equal(t, types.NewBigintValue(1), v)
				i++
				return nil
			})
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, len(test.expected), i)
				test.expected.RequireEqual(t, got)
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, `table.Scan("test", [{"min": (1), "max": (2)}])`, table.Scan("test", stream.Range{
			Min: testutil.ExprList(t, `(1)`), Max: testutil.ExprList(t, `(2)`),
		}).String())

		op := table.Scan("test",
			stream.Range{Min: testutil.ExprList(t, `(1)`), Max: testutil.ExprList(t, `(2)`), Exclusive: true},
			stream.Range{Min: testutil.ExprList(t, `(10)`), Exact: true},
			stream.Range{Min: testutil.ExprList(t, `(100)`)},
		)
		op.Reverse = true

		require.Equal(t, `table.ScanReverse("test", [{"min": (1), "max": (2), "exclusive": true}, {"min": (10), "exact": true}, {"min": (100)}])`, op.String())
	})
}
