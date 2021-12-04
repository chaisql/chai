package stream_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func TestExpressions(t *testing.T) {
	tests := []struct {
		e      expr.Expr
		output types.Document
		fails  bool
	}{
		{parser.MustParseExpr("3 + 4"), nil, true},
		{parser.MustParseExpr("{a: 3 + 4}"), testutil.MakeDocument(t, `{"a": 7}`), false},
	}

	for _, test := range tests {
		t.Run(test.e.String(), func(t *testing.T) {
			s := stream.New(stream.Expressions(test.e))

			err := s.Iterate(new(environment.Environment), func(env *environment.Environment) error {
				d, ok := env.GetDocument()
				require.True(t, ok)
				require.Equal(t, d, test.output)
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
		require.Equal(t, stream.Expressions(parser.MustParseExpr("1 + 1"), parser.MustParseExpr("pk()")).String(), "exprs(1 + 1, pk())")
	})
}

func TestSeqScan(t *testing.T) {
	tests := []struct {
		name                  string
		docsInTable, expected testutil.Docs
		reverse               bool
		fails                 bool
	}{
		{name: "empty"},
		{
			"ok",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			false,
			false,
		},
		{
			"reverse",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 2}`, `{"a": 1}`),
			true,
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, "CREATE TABLE test (a INTEGER)")

			for _, doc := range test.docsInTable {
				testutil.MustExec(t, db, tx, "INSERT INTO test VALUES ?", environment.Param{Value: doc})
			}

			op := stream.SeqScan("test")
			op.Reverse = test.reverse
			var in environment.Environment
			in.Tx = tx
			in.Catalog = db.Catalog

			var i int
			var got testutil.Docs
			err := op.Iterate(&in, func(env *environment.Environment) error {
				d, ok := env.GetDocument()
				require.True(t, ok)
				var fb document.FieldBuffer
				err := fb.Copy(d)
				assert.NoError(t, err)
				got = append(got, &fb)
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
		require.Equal(t, `seqScan(test)`, stream.SeqScan("test").String())
	})
}

func TestPkScan(t *testing.T) {
	tests := []struct {
		name                  string
		docsInTable, expected testutil.Docs
		ranges                stream.Ranges
		reverse               bool
		fails                 bool
	}{
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

			op := stream.PkScan("test", test.ranges...)
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
		require.Equal(t, `pkScan("test", [1, 2])`, stream.PkScan("test", stream.Range{
			Min: testutil.ExprList(t, `[1]`), Max: testutil.ExprList(t, `[2]`),
		}).String())

		op := stream.PkScan("test",
			stream.Range{Min: testutil.ExprList(t, `[1]`), Max: testutil.ExprList(t, `[2]`), Exclusive: true},
			stream.Range{Min: testutil.ExprList(t, `[10]`), Exact: true},
			stream.Range{Min: testutil.ExprList(t, `[100]`)},
		)
		op.Reverse = true

		require.Equal(t, `pkScanReverse("test", [1, 2, true], 10, [100, -1])`, op.String())
	})
}
