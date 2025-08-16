package stream_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/rows"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestStream(t *testing.T) {
	s := stream.New(rows.Emit(
		[]string{"a"},
		testutil.MakeRowExpr(t, `{"a": 1}`),
		testutil.MakeRowExpr(t, `{"a": 2}`),
	))

	s = s.Pipe(rows.Filter(parser.MustParseExpr("a > 1")))
	s = s.Pipe(rows.Project(parser.MustParseExpr("a + 1")))

	var count int64
	err := s.Iterate(new(environment.Environment), func(r database.Row) error {
		tt, err := json.Marshal(r)
		require.NoError(t, err)
		require.JSONEq(t, fmt.Sprintf(`{"a + 1": %d}`, count+3), string(tt))
		count++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestUnion(t *testing.T) {
	tests := []struct {
		name                 string
		first, second, third []expr.Row
		expected             testutil.Rows
	}{
		{
			"same rows",
			testutil.MakeRowExprs(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`),
			testutil.MakeRowExprs(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`),
			testutil.MakeRowExprs(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`),
			testutil.MakeRows(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`),
		},
		{
			"different rows",
			testutil.MakeRowExprs(t, `{"a": 1, "b": 1}`, `{"a": 1, "b": 2}`),
			testutil.MakeRowExprs(t, `{"a": 2, "b": 1}`, `{"a": 2, "b": 2}`),
			testutil.MakeRowExprs(t, `{"a": 3, "b": 1}`, `{"a": 3, "b": 2}`),
			testutil.MakeRows(t, `{"a": 1, "b": 1}`, `{"a": 1, "b": 2}`, `{"a": 2, "b": 1}`, `{"a": 2, "b": 2}`, `{"a": 3, "b": 1}`, `{"a": 3, "b": 2}`),
		},
		{
			"mixed",
			testutil.MakeRowExprs(t, `{"a": 1}`, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeRowExprs(t, `{"a": 1}`, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeRowExprs(t, `{"a": 1}`, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
		},
		{
			"only one",
			testutil.MakeRowExprs(t, `{"a": 1}`, `{"a": 1}`, `{"a": 2}`),
			nil, nil,
			testutil.MakeRows(t, `{"a": 1}`, `{"a": 2}`),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			var streams []*stream.Stream
			if test.first != nil {
				streams = append(streams, stream.New(rows.Emit([]string{"a", "b"}, test.first...)))
			}
			if test.second != nil {
				streams = append(streams, stream.New(rows.Emit([]string{"a", "b"}, test.second...)))
			}
			if test.third != nil {
				streams = append(streams, stream.New(rows.Emit([]string{"a", "b"}, test.third...)))
			}

			st := stream.New(stream.Union(streams...))
			env := environment.New(db, tx, nil, nil)

			test.expected.RequireEqualStream(t, env, st)
		})
	}

	t.Run("String", func(t *testing.T) {
		st := stream.New(stream.Union(
			stream.New(rows.Emit([]string{"a"}, testutil.MakeRowExprs(t, `{"a": 1}`, `{"a": 2}`)...)),
			stream.New(rows.Emit([]string{"a"}, testutil.MakeRowExprs(t, `{"a": 3}`, `{"a": 4}`)...)),
			stream.New(rows.Emit([]string{"a"}, testutil.MakeRowExprs(t, `{"a": 5}`, `{"a": 6}`)...)),
		))

		require.Equal(t, `union(rows.Emit((1), (2)), rows.Emit((3), (4)), rows.Emit((5), (6)))`, st.String())
	})
}

func TestConcatOperator(t *testing.T) {
	in1 := testutil.MakeRowExprs(t, `{"a": 10}`, `{"a": 11}`)
	in2 := testutil.MakeRowExprs(t, `{"a": 12}`, `{"a": 13}`)

	s1 := stream.New(rows.Emit([]string{"a"}, in1...))
	s2 := stream.New(rows.Emit([]string{"a"}, in2...))
	s := stream.New(stream.Concat(s1, s2))

	var got []row.Row
	err := s.Iterate(new(environment.Environment), func(r database.Row) error {
		var fb row.ColumnBuffer
		err := fb.Copy(r)
		if err != nil {
			return err
		}
		got = append(got, &fb)
		return nil
	})
	require.NoError(t, err)

	want := append(in1, in2...)
	for i, w := range want {
		r, _ := w.Eval(new(environment.Environment))
		testutil.RequireRowEqual(t, r, got[i])
	}
}
