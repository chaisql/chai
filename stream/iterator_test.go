package stream_test

import (
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/parser"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/genjidb/genji/stream"
	"github.com/genjidb/genji/testutil"
	"github.com/stretchr/testify/require"
)

func TestSeqScan(t *testing.T) {
	tests := []struct {
		name                  string
		docsInTable, expected testutil.Docs
		fails                 bool
	}{
		{name: "empty"},
		{
			"ok",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
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
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			nil, false, false,
		},
		{
			"max",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 1}`),
			stream.Ranges{
				{Max: parser.MustParseExpr("2")},
			},
			false, false,
		},
		{
			"min",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			stream.Ranges{
				{Min: parser.MustParseExpr("1")},
			},
			false, false,
		},
		{
			"min/max",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 1}`),
			stream.Ranges{
				{Min: parser.MustParseExpr("1"), Max: parser.MustParseExpr("2")},
			},
			false, false,
		},
		{
			"reverse/no range",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 2}`, `{"a": 1}`),
			nil, true, false,
		},
		{
			"reverse/max",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 2}`, `{"a": 1}`),
			stream.Ranges{
				{Max: parser.MustParseExpr("2")},
			},
			true, false,
		},
		{
			"reverse/min",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 2}`),
			stream.Ranges{
				{Min: parser.MustParseExpr("1")},
			},
			true, false,
		},
		{
			"reverse/min/max",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 2}`),
			stream.Ranges{
				{Min: parser.MustParseExpr("1"), Max: parser.MustParseExpr("2")},
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
		require.Equal(t, `pkScan('test', [1, 2])`, stream.PkScan("test", stream.Range{
			Min: parser.MustParseExpr("1"), Max: parser.MustParseExpr("2"),
		}).String())

		op := stream.PkScan("test",
			stream.Range{Min: parser.MustParseExpr("1"), Max: parser.MustParseExpr("2"), Exclusive: true},
			stream.Range{Min: parser.MustParseExpr("10"), Exact: true},
			stream.Range{Min: parser.MustParseExpr("100")},
		)
		op.Reverse = true

		require.Equal(t, `pkScanReverse('test', [1, 2, true], 10, [100, -1])`, op.String())
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
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			nil, false, false,
		},
		{
			"max",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 1}`),
			stream.Ranges{
				{Max: parser.MustParseExpr("2")},
			},
			false, false,
		},
		{
			"min",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			stream.Ranges{
				{Min: parser.MustParseExpr("1")},
			},
			false, false,
		},
		{
			"min/max",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 1}`),
			stream.Ranges{
				{Min: parser.MustParseExpr("1"), Max: parser.MustParseExpr("2")},
			},
			false, false,
		},
		{
			"reverse/no range",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 2}`, `{"a": 1}`),
			nil, true, false,
		},
		{
			"reverse/max",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 2}`, `{"a": 1}`),
			stream.Ranges{
				{Max: parser.MustParseExpr("2")},
			},
			true, false,
		},
		{
			"reverse/min",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 2}`),
			stream.Ranges{
				{Min: parser.MustParseExpr("1")},
			},
			true, false,
		},
		{
			"reverse/min/max",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 2}`),
			stream.Ranges{
				{Min: parser.MustParseExpr("1"), Max: parser.MustParseExpr("2")},
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

			op := stream.IndexScan("test", test.ranges...)
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
		require.Equal(t, `+test[1:2]`, stream.PkScan("test", stream.Range{
			Min: parser.MustParseExpr("1"), Max: parser.MustParseExpr("2"),
		}).String())

		op := stream.PkScan("test", stream.Range{
			Min: parser.MustParseExpr("1"), Max: parser.MustParseExpr("2"),
		})
		op.Reverse = true

		require.Equal(t, `-test[1:]`, op.String())
	})
}
