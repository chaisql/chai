package stream_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func TestTableScan(t *testing.T) {
	tests := []struct {
		name                  string
		docsInTable, expected testutil.Docs
		ranges                stream.Ranges
		reverse               bool
		fails                 bool
	}{
		{
			"no-range",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			nil,
			false,
			false,
		},
		{
			"no-range:reverse",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 2}`, `{"a": 1}`),
			nil,
			true,
			false,
		},
		{
			"max:2",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			stream.Ranges{
				{Max: testutil.ExprList(t, `[2]`)},
			},
			false, false,
		},
		{
			"max:1",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`),
			stream.Ranges{
				{Max: testutil.ExprList(t, `[1]`)},
			},
			false, false,
		},
		{
			"max:1.1",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`),
			stream.Ranges{
				{Max: testutil.ExprList(t, `[1.1]`)},
			},
			false, false,
		},
		{
			"min",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			stream.Ranges{
				{Min: testutil.ExprList(t, `[1]`)},
			},
			false, false,
		},
		{
			"min:0.5",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			stream.Ranges{
				{Min: testutil.ExprList(t, `[0.5]`)},
			},
			false, false,
		},
		{
			"min/max",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			stream.Ranges{
				{Min: testutil.ExprList(t, `[1]`), Max: testutil.ExprList(t, `[2]`)},
			},
			false, false,
		},
		{
			"min/max:0.5/1.5",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			stream.Ranges{
				{Min: testutil.ExprList(t, `[0.5]`), Max: testutil.ExprList(t, `[1.5]`)},
			},
			false, false,
		},
		{
			"reverse/max",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 2}`, `{"a": 1}`),
			stream.Ranges{
				{Max: testutil.ExprList(t, `[2]`)},
			},
			true, false,
		},
		{
			"reverse/min",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 2}`, `{"a": 1}`),
			stream.Ranges{
				{Min: testutil.ExprList(t, `[1]`)},
			},
			true, false,
		},
		{
			"reverse/min/max",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 2}`, `{"a": 1}`),
			stream.Ranges{
				{Min: testutil.ExprList(t, `[1]`), Max: testutil.ExprList(t, `[2]`)},
			},
			true, false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, "CREATE TABLE test (a INTEGER NOT NULL PRIMARY KEY)")

			for _, doc := range test.docsInTable {
				testutil.MustExec(t, db, tx, "INSERT INTO test VALUES ?", environment.Param{Value: doc})
			}

			op := stream.TableScan("test", test.ranges...)
			op.Reverse = test.reverse
			var env environment.Environment
			env.Tx = tx
			env.Catalog = db.Catalog
			env.Params = []environment.Param{{Name: "foo", Value: 1}}

			var i int
			var got testutil.Docs
			err := op.Iterate(&env, func(env *environment.Environment) error {
				d, ok := env.GetDocument()
				require.True(t, ok)
				var fb document.FieldBuffer

				err := fb.Copy(d)
				assert.NoError(t, err)

				got = append(got, &fb)
				v, err := env.GetParamByName("foo")
				assert.NoError(t, err)
				require.Equal(t, types.NewIntegerValue(1), v)
				i++
				return nil
			})
			if test.fails {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				require.Equal(t, len(test.expected), i)
				test.expected.RequireEqual(t, got)
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, `table.Scan("test", [{"min": [1], "max": [2]}])`, stream.TableScan("test", stream.Range{
			Min: testutil.ExprList(t, `[1]`), Max: testutil.ExprList(t, `[2]`),
		}).String())

		op := stream.TableScan("test",
			stream.Range{Min: testutil.ExprList(t, `[1]`), Max: testutil.ExprList(t, `[2]`), Exclusive: true},
			stream.Range{Min: testutil.ExprList(t, `[10]`), Exact: true},
			stream.Range{Min: testutil.ExprList(t, `[100]`)},
		)
		op.Reverse = true

		require.Equal(t, `table.ScanReverse("test", [{"min": [1], "max": [2], "exclusive": true}, {"min": [10], "exact": true}, {"min": [100]}])`, op.String())
	})
}
