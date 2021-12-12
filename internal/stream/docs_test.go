package stream_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/expr/functions"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func TestDocsEmit(t *testing.T) {
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
			s := stream.New(stream.DocsEmit(test.e))

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
		require.Equal(t, stream.DocsEmit(parser.MustParseExpr("1 + 1"), parser.MustParseExpr("pk()")).String(), "docs.Emit(1 + 1, pk())")
	})
}

func TestProject(t *testing.T) {
	tests := []struct {
		name  string
		exprs []expr.Expr
		in    types.Document
		out   string
		fails bool
	}{
		{
			"Constant",
			[]expr.Expr{parser.MustParseExpr("10")},
			testutil.MakeDocument(t, `{"a":1,"b":[true]}`),
			`{"10":10}`,
			false,
		},
		{
			"Wildcard",
			[]expr.Expr{expr.Wildcard{}},
			testutil.MakeDocument(t, `{"a":1,"b":[true]}`),
			`{"a":1,"b":[true]}`,
			false,
		},
		{
			"Multiple",
			[]expr.Expr{expr.Wildcard{}, expr.Wildcard{}, parser.MustParseExpr("10")},
			testutil.MakeDocument(t, `{"a":1,"b":[true]}`),
			`{"a":1,"b":[true],"a":1,"b":[true],"10":10}`,
			false,
		},
		{
			"Named",
			[]expr.Expr{&expr.NamedExpr{Expr: parser.MustParseExpr("10"), ExprName: "foo"}},
			testutil.MakeDocument(t, `{"a":1,"b":[true]}`),
			`{"foo":10}`,
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var inEnv environment.Environment
			inEnv.SetDocument(test.in)

			err := stream.DocsProject(test.exprs...).Iterate(&inEnv, func(out *environment.Environment) error {
				require.Equal(t, &inEnv, out.GetOuter())
				d, ok := out.GetDocument()
				require.True(t, ok)
				tt, err := json.Marshal(types.NewDocumentValue(d))
				require.NoError(t, err)
				require.JSONEq(t, test.out, string(tt))

				err = d.Iterate(func(field string, want types.Value) error {
					got, err := d.GetByField(field)
					assert.NoError(t, err)
					require.Equal(t, want, got)
					return nil
				})
				assert.NoError(t, err)
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
		require.Equal(t, "docs.Project(1, *, *, 1 + 1)", stream.DocsProject(
			parser.MustParseExpr("1"),
			expr.Wildcard{},
			expr.Wildcard{},
			parser.MustParseExpr("1 +    1"),
		).String())
	})

	t.Run("No input", func(t *testing.T) {
		stream.DocsProject(parser.MustParseExpr("1 + 1")).Iterate(new(environment.Environment), func(out *environment.Environment) error {
			d, ok := out.GetDocument()
			require.True(t, ok)
			enc, err := document.MarshalJSON(d)
			assert.NoError(t, err)
			require.JSONEq(t, `{"1 + 1": 2}`, string(enc))
			return nil
		})
	})
}

func TestAggregate(t *testing.T) {
	tests := []struct {
		name     string
		groupBy  expr.Expr
		builders []expr.AggregatorBuilder
		in       []types.Document
		want     []types.Document
		fails    bool
	}{
		{
			"fake count",
			nil,
			makeAggregatorBuilders("agg"),
			[]types.Document{testutil.MakeDocument(t, `{"a": 10}`)},
			[]types.Document{testutil.MakeDocument(t, `{"agg": 1}`)},
			false,
		},
		{
			"count",
			nil,
			[]expr.AggregatorBuilder{&functions.Count{Wildcard: true}},
			[]types.Document{testutil.MakeDocument(t, `{"a": 10}`)},
			[]types.Document{testutil.MakeDocument(t, `{"COUNT(*)": 1}`)},
			false,
		},
		{
			"count/groupBy",
			parser.MustParseExpr("a % 2"),
			[]expr.AggregatorBuilder{&functions.Count{Expr: parser.MustParseExpr("a")}, &functions.Avg{Expr: parser.MustParseExpr("a")}},
			generateSeqDocs(t, 10),
			[]types.Document{testutil.MakeDocument(t, `{"a % 2": 0, "COUNT(a)": 5, "AVG(a)": 4.0}`), testutil.MakeDocument(t, `{"a % 2": 1, "COUNT(a)": 5, "AVG(a)": 5.0}`)},
			false,
		},
		{
			"count/noInput",
			nil,
			[]expr.AggregatorBuilder{&functions.Count{Expr: parser.MustParseExpr("a")}, &functions.Avg{Expr: parser.MustParseExpr("a")}},
			nil,
			[]types.Document{testutil.MakeDocument(t, `{"COUNT(a)": 0, "AVG(a)": 0.0}`)},
			false,
		},
		{
			"no aggregator",
			parser.MustParseExpr("a % 2"),
			nil,
			generateSeqDocs(t, 4),
			testutil.MakeDocuments(t, `{"a % 2": 0}`, `{"a % 2": 1}`),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, "CREATE TABLE test(a int)")

			for _, doc := range test.in {
				testutil.MustExec(t, db, tx, "INSERT INTO test VALUES ?", environment.Param{Value: doc})
			}

			var env environment.Environment
			env.DB = db
			env.Tx = tx
			env.Catalog = db.Catalog

			s := stream.New(stream.TableScan("test"))
			if test.groupBy != nil {
				s = s.Pipe(stream.DocsTempTreeSort(test.groupBy))
			}

			s = s.Pipe(stream.DocsGroupAggregate(test.groupBy, test.builders...))

			var got []types.Document
			err := s.Iterate(&env, func(env *environment.Environment) error {
				d, ok := env.GetDocument()
				require.True(t, ok)
				var fb document.FieldBuffer
				fb.Copy(d)
				got = append(got, &fb)
				return nil
			})
			if test.fails {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				require.Equal(t, test.want, got)
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, `docs.GroupAggregate(a % 2, a(), b())`, stream.DocsGroupAggregate(parser.MustParseExpr("a % 2"), makeAggregatorBuilders("a()", "b()")...).String())
		require.Equal(t, `docs.GroupAggregate(NULL, a(), b())`, stream.DocsGroupAggregate(nil, makeAggregatorBuilders("a()", "b()")...).String())
		require.Equal(t, `docs.GroupAggregate(a % 2)`, stream.DocsGroupAggregate(parser.MustParseExpr("a % 2")).String())
	})
}

type fakeAggregator struct {
	count int64
	name  string
}

func (f *fakeAggregator) Eval(env *environment.Environment) (types.Value, error) {
	return types.NewIntegerValue(f.count), nil
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

func TestTempTreeSort(t *testing.T) {
	tests := []struct {
		name     string
		sortExpr expr.Expr
		values   []types.Document
		want     []types.Document
		fails    bool
		desc     bool
	}{
		{
			"ASC",
			parser.MustParseExpr("a"),
			[]types.Document{
				testutil.MakeDocument(t, `{"a": 0}`),
				testutil.MakeDocument(t, `{"a": null}`),
				testutil.MakeDocument(t, `{"a": true}`),
			},
			[]types.Document{
				testutil.MakeDocument(t, `{"a": null}`),
				testutil.MakeDocument(t, `{"a": 0}`),
				testutil.MakeDocument(t, `{"a": 1}`),
			},
			false,
			false,
		},
		{
			"DESC",
			parser.MustParseExpr("a"),
			[]types.Document{
				testutil.MakeDocument(t, `{"a": 0}`),
				testutil.MakeDocument(t, `{"a": null}`),
				testutil.MakeDocument(t, `{"a": true}`),
			},
			[]types.Document{
				testutil.MakeDocument(t, `{"a": 1}`),
				testutil.MakeDocument(t, `{"a": 0}`),
				testutil.MakeDocument(t, `{"a": null}`),
			},
			false,
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, "CREATE TABLE test(a int)")

			for _, doc := range test.values {
				testutil.MustExec(t, db, tx, "INSERT INTO test VALUES ?", environment.Param{Value: doc})
			}

			var env environment.Environment
			env.DB = db
			env.Tx = tx
			env.Catalog = db.Catalog

			s := stream.New(stream.TableScan("test"))
			if test.desc {
				s = s.Pipe(stream.DocsTempTreeSortReverse(test.sortExpr))
			} else {
				s = s.Pipe(stream.DocsTempTreeSort(test.sortExpr))
			}

			var got []types.Document
			err := s.Iterate(&env, func(env *environment.Environment) error {
				d, ok := env.GetDocument()
				require.True(t, ok)
				fmt.Printf("%v\n", types.NewDocumentValue(d))

				fb := document.NewFieldBuffer()
				fb.Copy(d)
				got = append(got, fb)
				return nil
			})
			fmt.Println("-----")

			if test.fails {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				require.Equal(t, len(got), len(test.want))
				for i := range got {
					testutil.RequireDocEqual(t, test.want[i], got[i])
				}
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, `docs.TempTreeSort(a)`, stream.DocsTempTreeSort(parser.MustParseExpr("a")).String())
	})
}
