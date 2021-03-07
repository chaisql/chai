package stream_test

import (
	"fmt"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/expr"
	"github.com/genjidb/genji/sql/parser"
	"github.com/genjidb/genji/stream"
	"github.com/genjidb/genji/testutil"
	"github.com/stretchr/testify/require"
)

func TestExpressions(t *testing.T) {
	tests := []struct {
		e      expr.Expr
		output document.Document
		fails  bool
	}{
		{parser.MustParseExpr("3 + 4"), nil, true},
		{parser.MustParseExpr("{a: 3 + 4}"), testutil.MakeDocument(t, `{"a": 7}`), false},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s", test.e), func(t *testing.T) {
			s := stream.New(stream.Expressions(test.e))

			err := s.Iterate(new(expr.Environment), func(env *expr.Environment) error {
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
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec("CREATE TABLE test (a INTEGER)")
			require.NoError(t, err)

			for _, doc := range test.docsInTable {
				err = db.Exec("INSERT INTO test VALUES ?", doc)
				require.NoError(t, err)
			}

			tx, err := db.Begin(false)
			require.NoError(t, err)
			defer tx.Rollback()

			op := stream.SeqScan("test")
			op.Reverse = test.reverse
			var in expr.Environment
			in.Tx = tx.Transaction

			var i int
			var got testutil.Docs
			err = op.Iterate(&in, func(env *expr.Environment) error {
				d, ok := env.GetDocument()
				require.True(t, ok)
				var fb document.FieldBuffer
				err = fb.Copy(d)
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
		ranges                stream.Ranges
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
			stream.Ranges{
				{Max: document.NewIntegerValue(2)},
			},
			false, false,
		},
		{
			"max:1",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`),
			stream.Ranges{
				{Max: document.NewIntegerValue(1)},
			},
			false, false,
		},
		{
			"min",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			stream.Ranges{
				{Min: document.NewIntegerValue(1)},
			},
			false, false,
		},
		{
			"min/max",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			stream.Ranges{
				{Min: document.NewIntegerValue(1), Max: document.NewIntegerValue(2)},
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
			stream.Ranges{
				{Max: document.NewIntegerValue(2)},
			},
			true, false,
		},
		{
			"reverse/min",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 2}`, `{"a": 1}`),
			stream.Ranges{
				{Min: document.NewIntegerValue(1)},
			},
			true, false,
		},
		{
			"reverse/min/max",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 2}`, `{"a": 1}`),
			stream.Ranges{
				{Min: document.NewIntegerValue(1), Max: document.NewIntegerValue(2)},
			},
			true, false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec("CREATE TABLE test (a INTEGER NOT NULL PRIMARY KEY)")
			require.NoError(t, err)

			for _, doc := range test.docsInTable {
				err = db.Exec("INSERT INTO test VALUES ?", doc)
				require.NoError(t, err)
			}

			tx, err := db.Begin(false)
			require.NoError(t, err)
			defer tx.Rollback()

			op := stream.PkScan("test", test.ranges...)
			op.Reverse = test.reverse
			var env expr.Environment
			env.Tx = tx.Transaction
			env.Params = []expr.Param{{Name: "foo", Value: 1}}

			var i int
			var got testutil.Docs
			err = op.Iterate(&env, func(env *expr.Environment) error {
				d, ok := env.GetDocument()
				require.True(t, ok)
				var fb document.FieldBuffer

				err = fb.Copy(d)
				require.NoError(t, err)

				got = append(got, &fb)
				v, err := env.GetParamByName("foo")
				require.NoError(t, err)
				require.Equal(t, document.NewIntegerValue(1), v)
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
		require.Equal(t, `pkScan("test", [1, 2])`, stream.PkScan("test", stream.Range{
			Min: document.NewIntegerValue(1), Max: document.NewIntegerValue(2),
		}).String())

		op := stream.PkScan("test",
			stream.Range{Min: document.NewIntegerValue(1), Max: document.NewIntegerValue(2), Exclusive: true},
			stream.Range{Min: document.NewIntegerValue(10), Exact: true},
			stream.Range{Min: document.NewIntegerValue(100)},
		)
		op.Reverse = true

		require.Equal(t, `pkScanReverse("test", [1, 2, true], 10, [100, -1])`, op.String())
	})
}

func TestIndexScan(t *testing.T) {
	tests := []struct {
		name                  string
		docsInTable, expected testutil.Docs
		ranges                stream.Ranges
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
			stream.Ranges{
				{Max: document.NewIntegerValue(2)},
			},
			false, false,
		},
		{
			"max:1",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`),
			stream.Ranges{
				{Max: document.NewIntegerValue(1)},
			},
			false, false,
		},
		{
			"min",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			stream.Ranges{
				{Min: document.NewIntegerValue(1)},
			},
			false, false,
		},
		{
			"min/max",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			stream.Ranges{
				{Min: document.NewIntegerValue(1), Max: document.NewIntegerValue(2)},
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
			stream.Ranges{
				{Max: document.NewIntegerValue(2)},
			},
			true, false,
		},
		{
			"reverse/min",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 2}`, `{"a": 1}`),
			stream.Ranges{
				{Min: document.NewIntegerValue(1)},
			},
			true, false,
		},
		{
			"reverse/min/max",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 2}`, `{"a": 1}`),
			stream.Ranges{
				{Min: document.NewIntegerValue(1), Max: document.NewIntegerValue(2)},
			},
			true, false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec("CREATE TABLE test (a INTEGER); CREATE INDEX idx_test_a ON test(a)")
			require.NoError(t, err)

			for _, doc := range test.docsInTable {
				err = db.Exec("INSERT INTO test VALUES ?", doc)
				require.NoError(t, err)
			}

			tx, err := db.Begin(false)
			require.NoError(t, err)
			defer tx.Rollback()

			op := stream.IndexScan("idx_test_a", test.ranges...)
			op.Reverse = test.reverse
			var env expr.Environment
			env.Tx = tx.Transaction
			env.Params = []expr.Param{{Name: "foo", Value: 1}}

			var i int
			var got testutil.Docs
			err = op.Iterate(&env, func(env *expr.Environment) error {
				d, ok := env.GetDocument()
				require.True(t, ok)
				var fb document.FieldBuffer

				err = fb.Copy(d)
				require.NoError(t, err)

				got = append(got, &fb)
				v, err := env.GetParamByName("foo")
				require.NoError(t, err)
				require.Equal(t, document.NewIntegerValue(1), v)
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
		require.Equal(t, `indexScan("idx_test_a", [1, 2])`, stream.IndexScan("idx_test_a", stream.Range{
			Min: document.NewIntegerValue(1), Max: document.NewIntegerValue(2),
		}).String())

		op := stream.IndexScan("idx_test_a", stream.Range{
			Min: document.NewIntegerValue(1), Max: document.NewIntegerValue(2),
		})
		op.Reverse = true

		require.Equal(t, `indexScanReverse("idx_test_a", [1, 2])`, op.String())
	})
}
