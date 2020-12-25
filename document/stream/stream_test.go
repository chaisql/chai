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
