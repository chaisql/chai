package rows_test

import (
	"testing"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/expr/functions"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/rows"
	"github.com/chaisql/chai/internal/stream/table"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/types"
	"github.com/stretchr/testify/require"
)

func TestAggregate(t *testing.T) {
	tests := []struct {
		name     string
		groupBy  expr.Expr
		builders []expr.AggregatorBuilder
		in       []int
		want     []row.Row
		fails    bool
	}{
		{
			"fake count",
			nil,
			makeAggregatorBuilders("agg"),
			[]int{10},
			[]row.Row{testutil.MakeRow(t, `{"agg": 1}`)},
			false,
		},
		{
			"count",
			nil,
			[]expr.AggregatorBuilder{functions.NewCount(expr.Wildcard{})},
			[]int{10},
			[]row.Row{testutil.MakeRow(t, `{"COUNT(*)": 1}`)},
			false,
		},
		{
			"count/groupBy",
			parser.MustParseExpr("a % 2"),
			[]expr.AggregatorBuilder{&functions.Count{Expr: parser.MustParseExpr("a")}, &functions.Avg{Expr: parser.MustParseExpr("a")}},
			generateSeq(t, 10),
			[]row.Row{testutil.MakeRow(t, `{"a % 2": 0, "COUNT(a)": 5, "AVG(a)": 4.0}`), testutil.MakeRow(t, `{"a % 2": 1, "COUNT(a)": 5, "AVG(a)": 5.0}`)},
			false,
		},
		{
			"count/noInput",
			nil,
			[]expr.AggregatorBuilder{&functions.Count{Expr: parser.MustParseExpr("a")}, &functions.Avg{Expr: parser.MustParseExpr("a")}},
			nil,
			[]row.Row{testutil.MakeRow(t, `{"COUNT(a)": 0, "AVG(a)": 0.0}`)},
			false,
		},
		{
			"no aggregator",
			parser.MustParseExpr("a % 2"),
			nil,
			generateSeq(t, 4),
			testutil.MakeRows(t, `{"a % 2": 0}`, `{"a % 2": 1}`),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, "CREATE TABLE test(a int primary key)")

			for _, val := range test.in {
				testutil.MustExec(t, db, tx, "INSERT INTO test VALUES ($1)", environment.Param{Value: val})
			}

			env := environment.New(db, tx, nil, nil)

			s := stream.New(table.Scan("test"))
			if test.groupBy != nil {
				s = s.Pipe(rows.TempTreeSort(test.groupBy))
			}

			s = s.Pipe(rows.GroupAggregate(test.groupBy, test.builders...))

			var got []row.Row
			err := s.Iterate(env, func(r database.Row) error {
				var fb row.ColumnBuffer
				err := fb.Copy(r)
				if err != nil {
					return err
				}
				got = append(got, &fb)
				return nil
			})
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				for i, doc := range test.want {
					testutil.RequireRowEqual(t, doc, got[i])
				}

				require.Equal(t, len(test.want), len(got))
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, `rows.GroupAggregate(a % 2, a(), b())`, rows.GroupAggregate(parser.MustParseExpr("a % 2"), makeAggregatorBuilders("a()", "b()")...).String())
		require.Equal(t, `rows.GroupAggregate(NULL, a(), b())`, rows.GroupAggregate(nil, makeAggregatorBuilders("a()", "b()")...).String())
		require.Equal(t, `rows.GroupAggregate(a % 2)`, rows.GroupAggregate(parser.MustParseExpr("a % 2")).String())
	})
}

type fakeAggregator struct {
	count int64
	name  string
}

func (f *fakeAggregator) Eval(env *environment.Environment) (types.Value, error) {
	return types.NewBigintValue(f.count), nil
}

func (f *fakeAggregator) Aggregate(env *environment.Environment) error {
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
	expr.Expr
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

func generateSeq(t testing.TB, max int) (vals []int) {
	t.Helper()

	for i := 0; i < max; i++ {
		vals = append(vals, i)
	}

	return vals
}
