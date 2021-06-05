package stream_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestConcatOperator(t *testing.T) {
	in1 := []document.Document{
		testutil.MakeDocument(t, `{"a": 10}`),
		testutil.MakeDocument(t, `{"a": 11}`),
	}
	in2 := []document.Document{
		testutil.MakeDocument(t, `{"a": 12}`),
		testutil.MakeDocument(t, `{"a": 13}`),
	}

	s1 := stream.New(stream.Documents(in1...))
	s2 := stream.New(stream.Documents(in2...))
	s := stream.Concat(s1, s2)

	var got []document.Document
	s.Iterate(new(expr.Environment), func(env *expr.Environment) error {
		d, ok := env.GetDocument()
		require.True(t, ok)
		got = append(got, d)
		return nil
	})

	want := append(in1, in2...)
	for i, w := range want {
		testutil.RequireDocEqual(t, w, got[i])
	}
}
