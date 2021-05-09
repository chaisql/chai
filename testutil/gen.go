package testutil

import (
	"strings"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/expr"
	"github.com/genjidb/genji/query"
	"github.com/genjidb/genji/sql/parser"
	"github.com/stretchr/testify/require"
)

type DocsStream struct {
	*parser.Parser
	env *expr.Environment
}

func (ds *DocsStream) Next() (*document.Value, error) {
	exp, err := ds.Parser.ParseDocument()
	if err != nil {
		return nil, err
	}

	v, err := exp.Eval(ds.env)
	if err != nil {
		return nil, err
	}

	return &v, nil
}

func ParseDocsStream(stream string) *DocsStream {
	p := parser.NewParser(strings.NewReader(stream))
	env := expr.NewEnvironment(document.NewFieldBuffer())

	return &DocsStream{p, env}
}

func RequireStreamEq(t *testing.T, raw string, res *query.Result) {
	t.Helper()
	var count int
	docs := ParseDocsStream(raw)
	err := res.Iterate(func(d document.Document) error {
		count++

		want, err := docs.Next()
		require.NoError(t, err)
		got := document.NewDocumentValue(d)

		eq, err := want.IsEqual(got)

		require.NoError(t, err, "comparing %s and %s yielded an error: %e", want, got, err)
		require.Truef(t, eq, "want %s, got %s", want.String(), got.String())
		return err
	})
	require.NoError(t, err)

	// there should not be any document left
	total := count
	var missing []string
	for v, err := docs.Next(); err == nil; v, err = docs.Next() {
		missing = append(missing, v.String())
		total++
	}

	require.Equalf(t, total, count, "want %d documents but got %d instead.\nMissing documents:\n%s",
		total,
		count,
		strings.Join(missing, "\n"))
}
