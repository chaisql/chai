package stream_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/parser"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/genjidb/genji/stream"
	"github.com/stretchr/testify/require"
)

func TestAggregate(t *testing.T) {
	tests := []struct {
		name    string
		groupBy expr.Expr
		aggs    []stream.HashAggregator
		in      []document.Document
		want    []document.Document
		fails   bool
	}{
		{
			"count",
			nil,
			makeAggregators("agg"),
			[]document.Document{docFromJSON(`{"a": 10}`)},
			[]document.Document{docFromJSON(`{"agg": 1}`)},
			false,
		},
		{
			"count/groupBy",
			parser.MustParseExpr("a % 2"),
			makeAggregators("agg1", "agg2"),
			generateSeqDocs(10),
			[]document.Document{docFromJSON(`{"agg1": 5, "agg2": 5}`), docFromJSON(`{"agg1": 5, "agg2": 5}`)},
			false,
		},
		{
			"count/noInput",
			nil,
			makeAggregators("agg1", "agg2"),
			nil,
			[]document.Document{docFromJSON(`{"agg1": 0, "agg2": 0}`)},
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := stream.New(stream.NewDocumentIterator(test.in...))
			if test.groupBy != nil {
				s = s.Pipe(stream.GroupBy(test.groupBy))
			}

			s = s.Pipe(stream.HashAggregate(test.aggs...))

			var got []document.Document
			err := s.Iterate(new(expr.Environment), func(env *expr.Environment) error {
				d, ok := env.GetDocument()
				require.True(t, ok)
				var fb document.FieldBuffer
				fb.Copy(d)
				got = append(got, &fb)
				return nil
			})
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.want, got)
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, `aggregate(a(), b())`, stream.HashAggregate(makeAggregators("a()", "b()")...).String())
	})
}

type fakeAggregator struct {
	count int64
	name  string
}

func (f *fakeAggregator) Eval(env *expr.Environment) (document.Value, error) {
	return document.NewIntegerValue(f.count), nil
}

func (f *fakeAggregator) Aggregate(env *expr.Environment) error {
	f.count++
	return nil
}

func (f *fakeAggregator) Name() string {
	return f.name
}

// Clone creates a new aggregator will its internal state initialized.
func (f fakeAggregator) Clone() stream.HashAggregator {
	f.count = 0
	return &f
}

func makeAggregators(names ...string) []stream.HashAggregator {
	aggs := make([]stream.HashAggregator, len(names))
	for i := range names {
		aggs[i] = &fakeAggregator{
			name: names[i],
		}
	}

	return aggs
}
