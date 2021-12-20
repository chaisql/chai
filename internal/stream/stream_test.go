package stream_test

import (
	"encoding/json"
	"fmt"
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

func TestStream(t *testing.T) {
	s := stream.New(stream.DocsEmit(
		testutil.ParseExpr(t, `{"a": 1}`),
		testutil.ParseExpr(t, `{"a": 2}`),
	))

	s = s.Pipe(stream.DocsFilter(parser.MustParseExpr("a > 1")))
	s = s.Pipe(stream.DocsProject(parser.MustParseExpr("a + 1")))

	var count int64
	err := s.Iterate(new(environment.Environment), func(env *environment.Environment) error {
		d, ok := env.GetDocument()
		require.True(t, ok)
		tt, err := json.Marshal(d)
		require.NoError(t, err)
		require.JSONEq(t, fmt.Sprintf(`{"a + 1": %d}`, count+3), string(tt))
		count++
		return nil
	})
	assert.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestUnion(t *testing.T) {
	tests := []struct {
		name                 string
		first, second, third []expr.Expr
		expected             testutil.Docs
		fails                bool
	}{
		{
			"same docs",
			testutil.ParseExprs(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`),
			testutil.ParseExprs(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`),
			testutil.ParseExprs(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`, `{"a": 2, "b": 2}`),
			false,
		},
		{
			"different docs",
			testutil.ParseExprs(t, `{"a": 1, "b": 1}`, `{"a": 1, "b": 2}`),
			testutil.ParseExprs(t, `{"a": 2, "b": 1}`, `{"a": 2, "b": 2}`),
			testutil.ParseExprs(t, `{"a": 3, "b": 1}`, `{"a": 3, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`, `{"a": 1, "b": 2}`, `{"a": 2, "b": 1}`, `{"a": 2, "b": 2}`, `{"a": 3, "b": 1}`, `{"a": 3, "b": 2}`),
			false,
		},
		{
			"mixed",
			testutil.ParseExprs(t, `{"a": 1}`, `{"a": 1}`, `{"a": 2}`),
			testutil.ParseExprs(t, `{"a": 1}`, `{"a": 1}`, `{"a": 2}`),
			testutil.ParseExprs(t, `{"a": 1}`, `{"a": 1}`, `{"a": 2}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			false,
		},
		{
			"only one",
			testutil.ParseExprs(t, `{"a": 1}`, `{"a": 1}`, `{"a": 2}`),
			nil, nil,
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			var streams []*stream.Stream
			if test.first != nil {
				streams = append(streams, stream.New(stream.DocsEmit(test.first...)))
			}
			if test.second != nil {
				streams = append(streams, stream.New(stream.DocsEmit(test.second...)))
			}
			if test.third != nil {
				streams = append(streams, stream.New(stream.DocsEmit(test.third...)))
			}

			st := stream.New(stream.Union(streams...))
			var env environment.Environment
			env.Tx = tx
			env.DB = db
			env.Catalog = db.Catalog

			var i int
			var got testutil.Docs
			err := st.Iterate(&env, func(env *environment.Environment) error {
				d, ok := env.GetDocument()
				require.True(t, ok)

				clone, err := document.CloneValue(types.NewDocumentValue(d))
				if err != nil {
					return err
				}

				got = append(got, clone.V().(types.Document))
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
		st := stream.New(stream.Union(
			stream.New(stream.DocsEmit(testutil.ParseExprs(t, `{"a": 1}`, `{"a": 2}`)...)),
			stream.New(stream.DocsEmit(testutil.ParseExprs(t, `{"a": 3}`, `{"a": 4}`)...)),
			stream.New(stream.DocsEmit(testutil.ParseExprs(t, `{"a": 5}`, `{"a": 6}`)...)),
		))

		require.Equal(t, `union(docs.Emit({a: 1}, {a: 2}), docs.Emit({a: 3}, {a: 4}), docs.Emit({a: 5}, {a: 6}))`, st.String())
	})
}

func TestConcatOperator(t *testing.T) {
	in1 := testutil.ParseExprs(t, `{"a": 10}`, `{"a": 11}`)
	in2 := testutil.ParseExprs(t, `{"a": 12}`, `{"a": 13}`)

	s1 := stream.New(stream.DocsEmit(in1...))
	s2 := stream.New(stream.DocsEmit(in2...))
	s := stream.Concat(s1, s2)

	var got []types.Document
	s.Iterate(new(environment.Environment), func(env *environment.Environment) error {
		d, ok := env.GetDocument()
		require.True(t, ok)

		var fb document.FieldBuffer
		err := fb.Copy(d)
		if err != nil {
			return err
		}
		got = append(got, d)
		return nil
	})

	want := append(in1, in2...)
	for i, w := range want {
		v, _ := w.Eval(new(environment.Environment))
		d := v.V().(types.Document)
		testutil.RequireDocEqual(t, d, got[i])
	}
}
