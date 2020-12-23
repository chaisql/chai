package stream_test

import (
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

	var count int64
	err := s.Iterate(func(env *expr.Environment) error {
		v, ok := env.GetCurrentValue()
		require.True(t, ok)
		require.Equal(t, document.NewIntegerValue(count+2), v)
		count++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
}
