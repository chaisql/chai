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

func TestStream(t *testing.T) {
	s := stream.New(stream.NewValueIterator(
		document.NewIntegerValue(1),
		document.NewIntegerValue(2),
	))

	s = s.Pipe(stream.Map(parser.MustParseExpr("_v + 1")))
	s = s.Pipe(stream.Filter(parser.MustParseExpr("_v > 2")))

	var count int64
	err := s.Iterate(func(env *expr.Environment) error {
		v, ok := env.GetCurrentValue()
		require.True(t, ok)
		require.Equal(t, document.NewIntegerValue(count+3), v)
		count++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

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

			env, err := stream.Map(test.e).Op()(test.in)
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
			env, err := stream.Filter(test.e).Op()(test.in)
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
