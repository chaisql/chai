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
		name     string
		groupBy  expr.Expr
		builders []expr.AggregatorBuilder
		in       []document.Document
		want     []document.Document
		fails    bool
	}{
		{
			"count",
			nil,
			makeAggregatorBuilders("agg"),
			[]document.Document{docFromJSON(`{"a": 10}`)},
			[]document.Document{docFromJSON(`{"agg": 1}`)},
			false,
		},
		{
			"count/groupBy",
			parser.MustParseExpr("a % 2"),
			makeAggregatorBuilders("agg1", "agg2"),
			generateSeqDocs(10),
			[]document.Document{docFromJSON(`{"a % 2": 0, "agg1": 5, "agg2": 5}`), docFromJSON(`{"a % 2": 1, "agg1": 5, "agg2": 5}`)},
			false,
		},
		{
			"count/noInput",
			nil,
			makeAggregatorBuilders("agg1", "agg2"),
			nil,
			[]document.Document{docFromJSON(`{"agg1": 0, "agg2": 0}`)},
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := stream.New(stream.Documents(test.in...))
			if test.groupBy != nil {
				s = s.Pipe(stream.GroupBy(test.groupBy))
			}

			s = s.Pipe(stream.HashAggregate(test.builders...))

			var got []document.Document
			err := s.Op.Iterate(new(expr.Environment), func(env *expr.Environment) error {
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
		require.Equal(t, `aggregate(a(), b())`, stream.HashAggregate(makeAggregatorBuilders("a()", "b()")...).String())
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

func (f *fakeAggregator) String() string {
	return f.name
}

type fakeAggretatorBuilder struct {
	name string
}

func (f *fakeAggretatorBuilder) Aggregator() expr.Aggregator {
	return &fakeAggregator{
		name: f.name,
	}
}

func (f *fakeAggretatorBuilder) String() string {
	return f.name
}

func makeAggregatorBuilders(names ...string) []expr.AggregatorBuilder {
	aggs := make([]expr.AggregatorBuilder, len(names))
	for i := range names {
		aggs[i] = &fakeAggretatorBuilder{
			name: names[i],
		}
	}

	return aggs
}
