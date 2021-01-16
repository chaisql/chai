package stream_test

import (
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/genjidb/genji/stream"
	"github.com/genjidb/genji/testutil"
	"github.com/stretchr/testify/require"
)

func TestTableIterator(t *testing.T) {
	tests := []struct {
		name                  string
		docsInTable, expected testutil.Docs
		min, max              *document.Value
		reverse               bool
		fails                 bool
	}{
		{name: "empty"},
		{
			"no range",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			nil, nil, false, false,
		},
		{
			"max",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 1}`),
			nil, testutil.MakeValue(t, 2),
			false, false,
		},
		{
			"min",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeValue(t, 1), nil,
			false, false,
		},
		{
			"min/max",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 1}`),
			testutil.MakeValue(t, 1), testutil.MakeValue(t, 2),
			false, false,
		},
		{
			"reverse/no range",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 2}`, `{"a": 1}`),
			nil, nil, true, false,
		},
		{
			"reverse/max",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 2}`, `{"a": 1}`),
			nil, testutil.MakeValue(t, 2),
			true, false,
		},
		{
			"reverse/min",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 2}`),
			testutil.MakeValue(t, 1), nil,
			true, false,
		},
		{
			"reverse/min/max",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 2}`),
			testutil.MakeValue(t, 1), testutil.MakeValue(t, 2),
			true, false,
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

			opts := stream.SeqScanOptions{
				Reverse: test.reverse,
			}
			if test.min != nil {
				opts.Min = *test.min
			}
			if test.max != nil {
				opts.Max = *test.max
			}
			it := stream.SeqScanWithOptions("test", opts)
			s := stream.New(it)
			var env expr.Environment
			env.Tx = tx.Transaction
			env.Params = []expr.Param{{Name: "foo", Value: 1}}

			var i int
			var got testutil.Docs
			err = s.Iterate(&env, func(env *expr.Environment) error {
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
		require.Equal(t, `+test[1:2]`, stream.SeqScanWithOptions("test", stream.SeqScanOptions{
			Min: *testutil.MakeValue(t, 1),
			Max: *testutil.MakeValue(t, 2),
		}).String())

		require.Equal(t, `-test[1:]`, stream.SeqScanWithOptions("test", stream.SeqScanOptions{
			Min:     *testutil.MakeValue(t, 1),
			Reverse: true,
		}).String())

		require.Equal(t, `-test[:]`, stream.SeqScanWithOptions("test", stream.SeqScanOptions{
			Reverse: true,
		}).String())
	})
}

func TestIndexIterator(t *testing.T) {
	tests := []struct {
		name                  string
		docsInTable, expected testutil.Docs
		min, max              *document.Value
		reverse               bool
		fails                 bool
	}{
		{name: "empty"},
		{
			"no range",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			nil, nil, false, false,
		},
		{
			"max",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 1}`),
			nil, testutil.MakeValue(t, 2),
			false, false,
		},
		{
			"min",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeValue(t, 1), nil,
			false, false,
		},
		{
			"min/max",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 1}`),
			testutil.MakeValue(t, 1), testutil.MakeValue(t, 2),
			false, false,
		},
		{
			"reverse/no range",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 2}`, `{"a": 1}`),
			nil, nil, true, false,
		},
		{
			"reverse/max",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 2}`, `{"a": 1}`),
			nil, testutil.MakeValue(t, 2),
			true, false,
		},
		{
			"reverse/min",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 2}`),
			testutil.MakeValue(t, 1), nil,
			true, false,
		},
		{
			"reverse/min/max",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(`{"a": 2}`),
			testutil.MakeValue(t, 1), testutil.MakeValue(t, 2),
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

			opts := stream.IndexScanOptions{
				Reverse: test.reverse,
			}
			if test.min != nil {
				opts.Min = *test.min
			}
			if test.max != nil {
				opts.Max = *test.max
			}
			it := stream.IndexScanWithOptions("idx_test_a", opts)
			var env expr.Environment
			env.Tx = tx.Transaction
			env.Params = []expr.Param{{Name: "foo", Value: 1}}
			s := stream.New(it)

			var i int
			var got testutil.Docs
			err = s.Iterate(&env, func(env *expr.Environment) error {
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
		require.Equal(t, `+test[1:2]`, stream.IndexScanWithOptions("test", stream.IndexScanOptions{
			Min: *testutil.MakeValue(t, 1),
			Max: *testutil.MakeValue(t, 2),
		}).String())

		require.Equal(t, `-test[1:]`, stream.IndexScanWithOptions("test", stream.IndexScanOptions{
			Min:     *testutil.MakeValue(t, 1),
			Reverse: true,
		}).String())

		require.Equal(t, `-test[:]`, stream.IndexScanWithOptions("test", stream.IndexScanOptions{
			Reverse: true,
		}).String())
	})
}
