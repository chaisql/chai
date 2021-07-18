package stream_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/testutil"
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
				require.Error(t, err)
			} else {
				require.NoError(t, err)
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
				require.NoError(t, err)
				got = append(got, &fb)
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
		require.Equal(t, `seqScan(test)`, stream.SeqScan("test").String())
	})
}

func TestPkScan(t *testing.T) {
	tests := []struct {
		name                  string
		docsInTable, expected testutil.Docs
		ranges                stream.ValueRanges
		reverse               bool
		fails                 bool
	}{
		{name: "empty"},
		{
			"no range",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			nil, false, false,
		},
		{
			"max:2",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			stream.ValueRanges{
				{Max: testutil.IntegerValue(2)},
			},
			false, false,
		},
		{
			"max:1",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`),
			stream.ValueRanges{
				{Max: testutil.IntegerValue(1)},
			},
			false, false,
		},
		{
			"max:1.1",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`),
			stream.ValueRanges{
				{Max: testutil.DoubleValue(1.1)},
			},
			false, false,
		},
		{
			"min",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			stream.ValueRanges{
				{Min: testutil.IntegerValue(1)},
			},
			false, false,
		},
		{
			"min:0.5",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			stream.ValueRanges{
				{Min: testutil.DoubleValue(0.5)},
			},
			false, false,
		},
		{
			"min/max",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			stream.ValueRanges{
				{Min: testutil.IntegerValue(1), Max: testutil.IntegerValue(2)},
			},
			false, false,
		},
		{
			"min/max:0.5/1.5",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			stream.ValueRanges{
				{Min: testutil.DoubleValue(0.5), Max: testutil.DoubleValue(1.5)},
			},
			false, false,
		},
		{
			"reverse/no range",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 2}`, `{"a": 1}`),
			nil, true, false,
		},
		{
			"reverse/max",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 2}`, `{"a": 1}`),
			stream.ValueRanges{
				{Max: testutil.IntegerValue(2)},
			},
			true, false,
		},
		{
			"reverse/min",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 2}`, `{"a": 1}`),
			stream.ValueRanges{
				{Min: testutil.IntegerValue(1)},
			},
			true, false,
		},
		{
			"reverse/min/max",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 2}`, `{"a": 1}`),
			stream.ValueRanges{
				{Min: testutil.IntegerValue(1), Max: testutil.IntegerValue(2)},
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
				require.NoError(t, err)

				got = append(got, &fb)
				v, err := env.GetParamByName("foo")
				require.NoError(t, err)
				require.Equal(t, types.NewIntegerValue(1), v)
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
		require.Equal(t, `pkScan("test", [1, 2])`, stream.PkScan("test", stream.ValueRange{
			Min: testutil.IntegerValue(1), Max: testutil.IntegerValue(2),
		}).String())

		op := stream.PkScan("test",
			stream.ValueRange{Min: testutil.IntegerValue(1), Max: testutil.IntegerValue(2), Exclusive: true},
			stream.ValueRange{Min: testutil.IntegerValue(10), Exact: true},
			stream.ValueRange{Min: testutil.IntegerValue(100)},
		)
		op.Reverse = true

		require.Equal(t, `pkScanReverse("test", [1, 2, true], 10, [100, -1])`, op.String())
	})
}

func TestIndexScan(t *testing.T) {
	tests := []struct {
		name                  string
		indexOn               string
		docsInTable, expected testutil.Docs
		ranges                stream.IndexRanges
		reverse               bool
		fails                 bool
	}{
		{name: "empty", indexOn: "a"},
		{
			"no range", "a",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			nil, false, false,
		},
		{
			"no range", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 3}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 3}`),
			nil, false, false,
		},
		{
			"max:2", "a",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			stream.IndexRanges{
				{Max: testutil.ExprList(t, `[2]`), Paths: []document.Path{testutil.ParseDocumentPath(t, "a")}},
			},
			false, false,
		},
		{
			"max:1.2", "a",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`),
			stream.IndexRanges{
				{Max: testutil.ExprList(t, `[1.2]`), Paths: []document.Path{testutil.ParseDocumentPath(t, "a")}},
			},
			false, false,
		},
		{
			"max:[2, 2]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 2}`),
			stream.IndexRanges{
				{Max: testutil.ExprList(t, `[2, 2]`), Paths: []document.Path{testutil.ParseDocumentPath(t, "a"), testutil.ParseDocumentPath(t, "b")}},
			},
			false, false,
		},
		{
			"max:[2, 2.2]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 2}`),
			stream.IndexRanges{
				{Max: testutil.ExprList(t, `[2, 2.2]`), Paths: []document.Path{testutil.ParseDocumentPath(t, "a"), testutil.ParseDocumentPath(t, "b")}},
			},
			false, false,
		},
		{
			"max:1", "a",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`),
			stream.IndexRanges{
				{Max: testutil.ExprList(t, `[1]`), Paths: []document.Path{testutil.ParseDocumentPath(t, "a")}},
			},
			false, false,
		},
		{
			"max:[1, 2]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`),
			stream.IndexRanges{
				{Max: testutil.ExprList(t, `[1, 2]`), Paths: []document.Path{testutil.ParseDocumentPath(t, "a"), testutil.ParseDocumentPath(t, "b")}},
			},
			false, false,
		},
		{
			"max:[1.1, 2]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`),
			stream.IndexRanges{
				{Max: testutil.ExprList(t, `[1.1, 2]`), Paths: []document.Path{testutil.ParseDocumentPath(t, "a"), testutil.ParseDocumentPath(t, "b")}},
			},
			false, false,
		},
		{
			"min", "a",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			stream.IndexRanges{
				{Min: testutil.ExprList(t, `[1]`), Paths: []document.Path{testutil.ParseDocumentPath(t, "a")}},
			},
			false, false,
		},
		{
			"min:[2, 1]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 2, "b": 2}`),
			stream.IndexRanges{
				{
					Min:   testutil.ExprList(t, `[2, 1]`),
					Paths: []document.Path{testutil.ParseDocumentPath(t, "a"), testutil.ParseDocumentPath(t, "b")},
				},
			},
			false, false,
		},
		{
			"min:[2, 1.5]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 2, "b": 2}`),
			stream.IndexRanges{
				{
					Min:   testutil.ExprList(t, `[2, 1.5]`),
					Paths: []document.Path{testutil.ParseDocumentPath(t, "a"), testutil.ParseDocumentPath(t, "b")},
				},
			},
			false, false,
		},
		{
			"min/max", "a",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			stream.IndexRanges{
				{
					Min:   testutil.ExprList(t, `[1]`),
					Max:   testutil.ExprList(t, `[2]`),
					Paths: []document.Path{testutil.ParseDocumentPath(t, "a")},
				},
			},
			false, false,
		},
		{
			"min:[1, 1], max:[2,2]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`, `{"a": 2, "b": 2}`),
			stream.IndexRanges{
				{
					Min:   testutil.ExprList(t, `[1, 1]`),
					Max:   testutil.ExprList(t, `[2, 2]`),
					Paths: []document.Path{testutil.ParseDocumentPath(t, "a"), testutil.ParseDocumentPath(t, "b")},
				},
			},
			false, false,
		},
		{
			"min:[1, 1], max:[2,2] bis", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 3}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 3}`, `{"a": 2, "b": 2}`), // [1, 3] < [2, 2]
			stream.IndexRanges{
				{
					Min:   testutil.ExprList(t, `[1, 1]`),
					Max:   testutil.ExprList(t, `[2, 2]`),
					Paths: []document.Path{testutil.ParseDocumentPath(t, "a"), testutil.ParseDocumentPath(t, "b")},
				},
			},
			false, false,
		},
		{
			"reverse/no range", "a",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 2}`, `{"a": 1}`),
			nil, true, false,
		},
		{
			"reverse/max", "a",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 2}`, `{"a": 1}`),
			stream.IndexRanges{
				{Max: testutil.ExprList(t, `[2]`), Paths: []document.Path{testutil.ParseDocumentPath(t, "a")}},
			},
			true, false,
		},
		{
			"reverse/max", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 2, "b": 2}`, `{"a": 1, "b": 1}`),
			stream.IndexRanges{
				{
					Max:   testutil.ExprList(t, `[2, 2]`),
					Paths: []document.Path{testutil.ParseDocumentPath(t, "a"), testutil.ParseDocumentPath(t, "b")},
				},
			},
			true, false,
		},
		{
			"reverse/min", "a",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 2}`, `{"a": 1}`),
			stream.IndexRanges{
				{Min: testutil.ExprList(t, `[1]`), Paths: []document.Path{testutil.ParseDocumentPath(t, "a")}},
			},
			true, false,
		},
		{
			"reverse/min neg", "a",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": -2}`),
			testutil.MakeDocuments(t, `{"a": 1}`),
			stream.IndexRanges{
				{Min: testutil.ExprList(t, `[1]`), Paths: []document.Path{testutil.ParseDocumentPath(t, "a")}},
			},
			true, false,
		},
		{
			"reverse/min", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 2, "b": 2}`, `{"a": 1, "b": 1}`),
			stream.IndexRanges{
				{
					Min:   testutil.ExprList(t, `[1, 1]`),
					Paths: []document.Path{testutil.ParseDocumentPath(t, "a"), testutil.ParseDocumentPath(t, "b")},
				},
			},
			true, false,
		},
		{
			"reverse/min/max", "a",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 2}`, `{"a": 1}`),
			stream.IndexRanges{
				{
					Min:   testutil.ExprList(t, `[1]`),
					Max:   testutil.ExprList(t, `[2]`),
					Paths: []document.Path{testutil.ParseDocumentPath(t, "a")},
				},
			},
			true, false,
		},
		{
			"reverse/min/max", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 2, "b": 2}`, `{"a": 1, "b": 1}`),
			stream.IndexRanges{
				{
					Min:   testutil.ExprList(t, `[1, 1]`),
					Max:   testutil.ExprList(t, `[2, 2]`),
					Paths: []document.Path{testutil.ParseDocumentPath(t, "a"), testutil.ParseDocumentPath(t, "b")},
				},
			},
			true, false,
		},
		{
			"max:[1]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`, `{"a": 1, "b": 9223372036854775807}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`, `{"a": 1, "b": 9223372036854775807}`),
			stream.IndexRanges{
				{
					IndexArity: 2,
					Max:        testutil.ExprList(t, `[1]`),
					Paths:      []document.Path{testutil.ParseDocumentPath(t, "a"), testutil.ParseDocumentPath(t, "b")},
				},
			},
			false, false,
		},
		{
			"reverse max:[1]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`, `{"a": 1, "b": 9223372036854775807}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 9223372036854775807}`, `{"a": 1, "b": 1}`),
			stream.IndexRanges{
				{
					Max:        testutil.ExprList(t, `[1]`),
					Exclusive:  false,
					Exact:      false,
					IndexArity: 2,
					Paths:      []document.Path{testutil.ParseDocumentPath(t, "a"), testutil.ParseDocumentPath(t, "b")},
				},
			},
			true, false,
		},
		{
			"max:[1, 2]", "a, b, c",
			testutil.MakeDocuments(t, `{"a": 1, "b": 2, "c": 1}`, `{"a": 2, "b": 2, "c":  2}`, `{"a": 1, "b": 2, "c": 9223372036854775807}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 2, "c": 1}`, `{"a": 1, "b": 2, "c": 9223372036854775807}`),
			stream.IndexRanges{
				{
					IndexArity: 3, Max: testutil.ExprList(t, `[1, 2]`), Paths: []document.Path{testutil.ParseDocumentPath(t, "a"), testutil.ParseDocumentPath(t, "b"), testutil.ParseDocumentPath(t, "c")},
				},
			},
			false, false,
		},
		{
			"min:[1]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": -2}`, `{"a": -2, "b": 2}`, `{"a": 1, "b": 1}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": -2}`, `{"a": 1, "b": 1}`),
			stream.IndexRanges{
				{Min: testutil.ExprList(t, `[1]`), Paths: []document.Path{testutil.ParseDocumentPath(t, "a"), testutil.ParseDocumentPath(t, "b")}},
			},
			false, false,
		},
		{
			"min:[1]", "a, b, c",
			testutil.MakeDocuments(t, `{"a": 1, "b": -2, "c": 0}`, `{"a": -2, "b": 2, "c": 1}`, `{"a": 1, "b": 1, "c": 2}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": -2, "c": 0}`, `{"a": 1, "b": 1, "c": 2}`),
			stream.IndexRanges{
				{Min: testutil.ExprList(t, `[1]`), Paths: []document.Path{testutil.ParseDocumentPath(t, "a"), testutil.ParseDocumentPath(t, "b"), testutil.ParseDocumentPath(t, "c")}},
			},
			false, false,
		},
		{
			"reverse min:[1]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": -2}`, `{"a": -2, "b": 2}`, `{"a": 1, "b": 1}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`, `{"a": 1, "b": -2}`),
			stream.IndexRanges{
				{Min: testutil.ExprList(t, `[1]`), Paths: []document.Path{testutil.ParseDocumentPath(t, "a"), testutil.ParseDocumentPath(t, "b")}},
			},
			true, false,
		},
		{
			"min:[1], max[2]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": -2}`, `{"a": -2, "b": 2}`, `{"a": 2, "b": 42}`, `{"a": 3, "b": -1}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": -2}`, `{"a": 2, "b": 42}`),
			stream.IndexRanges{
				{
					IndexArity: 2,
					Min:        testutil.ExprList(t, `[1]`),
					Max:        testutil.ExprList(t, `[2]`),
					Paths:      []document.Path{testutil.ParseDocumentPath(t, "a"), testutil.ParseDocumentPath(t, "b")},
				},
			},
			false, false,
		},
		{
			"reverse min:[1], max[2]", "a, b",
			testutil.MakeDocuments(t, `{"a": 1, "b": -2}`, `{"a": -2, "b": 2}`, `{"a": 2, "b": 42}`, `{"a": 3, "b": -1}`),
			testutil.MakeDocuments(t, `{"a": 2, "b": 42}`, `{"a": 1, "b": -2}`),
			stream.IndexRanges{
				{
					IndexArity: 2,
					Min:        testutil.ExprList(t, `[1]`),
					Max:        testutil.ExprList(t, `[2]`),
					Paths:      []document.Path{testutil.ParseDocumentPath(t, "a"), testutil.ParseDocumentPath(t, "b")},
				},
			},
			true, false,
		},
	}

	for _, test := range tests {
		t.Run(test.name+":index on "+test.indexOn, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, "CREATE TABLE test (a INTEGER, b INTEGER, c INTEGER); CREATE INDEX idx_test_a ON test("+test.indexOn+")")

			for _, doc := range test.docsInTable {
				testutil.MustExec(t, db, tx, "INSERT INTO test VALUES ?", environment.Param{Value: doc})
			}

			op := stream.IndexScan("idx_test_a", test.ranges...)
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
				require.NoError(t, err)

				got = append(got, &fb)
				v, err := env.GetParamByName("foo")
				require.NoError(t, err)
				require.Equal(t, types.NewIntegerValue(1), v)
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
		t.Run("idx_test_a", func(t *testing.T) {
			require.Equal(t, `indexScan("idx_test_a", [1, 2])`, stream.IndexScan("idx_test_a", stream.IndexRange{
				Min: testutil.ExprList(t, `[1]`), Max: testutil.ExprList(t, `[2]`),
			}).String())

			op := stream.IndexScan("idx_test_a", stream.IndexRange{
				Min: testutil.ExprList(t, `[1]`), Max: testutil.ExprList(t, `[2]`),
			})
			op.Reverse = true

			require.Equal(t, `indexScanReverse("idx_test_a", [1, 2])`, op.String())
		})

		t.Run("idx_test_a_b", func(t *testing.T) {
			require.Equal(t, `indexScan("idx_test_a_b", [[1, 1], [2, 2]])`, stream.IndexScan("idx_test_a_b", stream.IndexRange{
				Min: testutil.ExprList(t, `[1, 1]`),
				Max: testutil.ExprList(t, `[2, 2]`),
			}).String())

			op := stream.IndexScan("idx_test_a_b", stream.IndexRange{
				Min: testutil.ExprList(t, `[1, 1]`),
				Max: testutil.ExprList(t, `[2, 2]`),
			})
			op.Reverse = true

			require.Equal(t, `indexScanReverse("idx_test_a_b", [[1, 1], [2, 2]])`, op.String())
		})
	})
}
