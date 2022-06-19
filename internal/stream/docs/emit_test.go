package docs_test

import (
	"testing"

	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/stream/docs"
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
			s := stream.New(docs.Emit(test.e))

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
		require.Equal(t, docs.Emit(parser.MustParseExpr("1 + 1"), parser.MustParseExpr("pk()")).String(), "docs.Emit(1 + 1, pk())")
	})
}
