package stream_test

import (
	"fmt"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/stream"
	"github.com/genjidb/genji/sql/parser"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/stretchr/testify/require"
)

func TestMap(t *testing.T) {
	tests := []struct {
		e       expr.Expr
		in, out *expr.Environment
		fails   bool
	}{
		{
			parser.MustParseExpr("10"),
			expr.NewEnvironment(document.NewIntegerValue(1)),
			expr.NewEnvironment(document.NewIntegerValue(10)),
			false,
		},
		{
			parser.MustParseExpr("null"),
			expr.NewEnvironment(document.NewIntegerValue(1)),
			expr.NewEnvironment(document.NewNullValue()),
			false,
		},
		{
			parser.MustParseExpr("a"),
			expr.NewEnvironment(document.NewIntegerValue(1)),
			expr.NewEnvironment(document.NewNullValue()),
			false,
		},
		{
			parser.MustParseExpr("a"),
			expr.NewEnvironment(document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntegerValue(1)))),
			expr.NewEnvironment(document.NewIntegerValue(1)),
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s", test.e), func(t *testing.T) {
			test.out.Outer = test.in

			op, err := stream.Map(test.e).Op()
			require.NoError(t, err)
			env, err := op(test.in)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.out, env)
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, stream.Map(parser.MustParseExpr("1")).String(), "map(1)")
	})
}

func TestFilter(t *testing.T) {
	tests := []struct {
		e       expr.Expr
		in, out *expr.Environment
		fails   bool
	}{
		{
			parser.MustParseExpr("1"),
			expr.NewEnvironment(document.NewIntegerValue(1)),
			expr.NewEnvironment(document.NewIntegerValue(1)),
			false,
		},
		{
			parser.MustParseExpr("_v > 1"),
			expr.NewEnvironment(document.NewIntegerValue(1)),
			nil,
			false,
		},
		{
			parser.MustParseExpr("_v >= 1"),
			expr.NewEnvironment(document.NewIntegerValue(1)),
			expr.NewEnvironment(document.NewIntegerValue(1)),
			false,
		},
		{
			parser.MustParseExpr("null"),
			expr.NewEnvironment(document.NewIntegerValue(1)),
			nil,
			false,
		},
		{
			parser.MustParseExpr("a"),
			expr.NewEnvironment(document.NewIntegerValue(1)),
			nil,
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s", test.e), func(t *testing.T) {
			op, err := stream.Filter(test.e).Op()
			require.NoError(t, err)
			env, err := op(test.in)

			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.out, env)
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, stream.Filter(parser.MustParseExpr("1")).String(), "filter(1)")
	})
}

func TestTake(t *testing.T) {
	tests := []struct {
		inNumber int64
		n        expr.Expr
		output   int
		fails    bool
	}{
		{5, parser.MustParseExpr("1"), 1, false},
		{5, parser.MustParseExpr("7"), 5, false},
		{5, parser.MustParseExpr("1.1"), 1, false},
		{5, parser.MustParseExpr("true"), 1, false},
		{5, parser.MustParseExpr("1 + 1"), 2, false},
		{5, parser.MustParseExpr("a"), 1, true},
		{5, parser.MustParseExpr("'hello'"), 1, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%d/%s", test.inNumber, test.n), func(t *testing.T) {
			var values []document.Value

			for i := int64(0); i < test.inNumber; i++ {
				values = append(values, document.NewIntegerValue(i))
			}

			s := stream.New(stream.NewValueIterator(values...))
			s = s.Pipe(stream.Take(test.n))

			var count int
			err := s.Iterate(func(env *expr.Environment) error {
				count++
				return nil
			})
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.output, count)
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, stream.Take(parser.MustParseExpr("1")).String(), "take(1)")
	})
}

func TestSkip(t *testing.T) {
	tests := []struct {
		inNumber int64
		n        expr.Expr
		output   int
		fails    bool
	}{
		{5, parser.MustParseExpr("1"), 4, false},
		{5, parser.MustParseExpr("7"), 0, false},
		{5, parser.MustParseExpr("1.1"), 4, false},
		{5, parser.MustParseExpr("true"), 4, false},
		{5, parser.MustParseExpr("1 + 1"), 3, false},
		{5, parser.MustParseExpr("a"), 1, true},
		{5, parser.MustParseExpr("'hello'"), 1, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%d/%s", test.inNumber, test.n), func(t *testing.T) {
			var values []document.Value

			for i := int64(0); i < test.inNumber; i++ {
				values = append(values, document.NewIntegerValue(i))
			}

			s := stream.New(stream.NewValueIterator(values...))
			s = s.Pipe(stream.Skip(test.n))

			var count int
			err := s.Iterate(func(env *expr.Environment) error {
				count++
				return nil
			})
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.output, count)
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, stream.Skip(parser.MustParseExpr("1")).String(), "skip(1)")
	})
}

func TestGroupBy(t *testing.T) {
	tests := []struct {
		e     expr.Expr
		in    *expr.Environment
		group document.Value
		fails bool
	}{
		{
			parser.MustParseExpr("10"),
			expr.NewEnvironment(document.NewIntegerValue(1)),
			document.NewIntegerValue(10),
			false,
		},
		{
			parser.MustParseExpr("null"),
			expr.NewEnvironment(document.NewIntegerValue(1)),
			document.NewNullValue(),
			false,
		},
		{
			parser.MustParseExpr("a"),
			expr.NewEnvironment(document.NewIntegerValue(1)),
			document.NewNullValue(),
			false,
		},
		{
			parser.MustParseExpr("_v"),
			expr.NewEnvironment(document.NewIntegerValue(1)),
			document.NewIntegerValue(1),
			false,
		},
		{
			parser.MustParseExpr("a"),
			expr.NewEnvironment(document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntegerValue(1)))),
			document.NewIntegerValue(1),
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s", test.e), func(t *testing.T) {
			var want expr.Environment
			want.Outer = test.in
			want.Set("_group", test.group)

			op, err := stream.GroupBy(test.e).Op()
			require.NoError(t, err)
			env, err := op(test.in)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, &want, env)
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, stream.GroupBy(parser.MustParseExpr("1")).String(), "groupBy(1)")
	})
}

func generateSeqValues(max int64) (values []document.Value) {
	for i := int64(0); i < max; i++ {
		values = append(values, document.NewIntegerValue(i))
	}

	return values
}

func TestReduce(t *testing.T) {
	tests := []struct {
		name      string
		groupBy   expr.Expr
		seed, acc expr.Expr
		values    []document.Value
		want      []document.Value
		fails     bool
	}{
		{
			"count",
			nil,
			parser.MustParseExpr("0"),
			parser.MustParseExpr("_acc + 1"),
			[]document.Value{document.NewIntegerValue(0)},
			[]document.Value{document.NewIntegerValue(1)},
			false,
		},
		{
			"count/groupBy",
			parser.MustParseExpr("_v % 2"),
			parser.MustParseExpr("0"),
			parser.MustParseExpr("_acc + 1"),
			generateSeqValues(10),
			[]document.Value{document.NewIntegerValue(5), document.NewIntegerValue(5)},
			false,
		},
		{
			"count/noInput",
			nil,
			parser.MustParseExpr("0"),
			parser.MustParseExpr("_acc + 1"),
			nil,
			[]document.Value{document.NewIntegerValue(0)},
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := stream.New(stream.NewValueIterator(test.values...))
			if test.groupBy != nil {
				s = s.Pipe(stream.GroupBy(test.groupBy))
			}
			s = s.Pipe(stream.Reduce(test.seed, test.acc))

			var got []document.Value
			err := s.Iterate(func(env *expr.Environment) error {
				v, ok := env.GetCurrentValue()
				require.True(t, ok)
				got = append(got, v)
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
		require.Equal(t, `reduce({"count": 0}, {"count": _acc + 1})`, stream.Reduce(parser.MustParseExpr("{count: 0}"), parser.MustParseExpr("{count: _acc +1}")).String())
	})
}

func TestSort(t *testing.T) {
	tests := []struct {
		name     string
		sortExpr expr.Expr
		values   []document.Value
		want     []document.Value
		fails    bool
		desc     bool
	}{
		{
			"ASC",
			parser.MustParseExpr("_v"),
			[]document.Value{
				document.NewIntegerValue(0),
				document.NewNullValue(),
				document.NewBoolValue(true),
			},
			[]document.Value{
				document.NewNullValue(),
				document.NewBoolValue(true),
				document.NewIntegerValue(0),
			},
			false,
			false,
		},
		{
			"DESC",
			parser.MustParseExpr("_v"),
			[]document.Value{
				document.NewIntegerValue(0),
				document.NewNullValue(),
				document.NewBoolValue(true),
			},
			[]document.Value{
				document.NewIntegerValue(0),
				document.NewBoolValue(true),
				document.NewNullValue(),
			},
			false,
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := stream.New(stream.NewValueIterator(test.values...))
			if test.desc {
				s = s.Pipe(stream.SortReverse(test.sortExpr))
			} else {
				s = s.Pipe(stream.Sort(test.sortExpr))
			}

			var got []document.Value
			err := s.Iterate(func(env *expr.Environment) error {
				v, ok := env.GetCurrentValue()
				require.True(t, ok)
				got = append(got, v)
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
		require.Equal(t, `sort(a)`, stream.Sort(parser.MustParseExpr("a")).String())
	})
}
