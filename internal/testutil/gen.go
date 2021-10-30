package testutil

import (
	"errors"
	"strings"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

type ResultStream struct {
	*parser.Parser
	env *environment.Environment
}

func (ds *ResultStream) Next() (types.Value, error) {
	exp, err := ds.Parser.ParseDocument()
	if err != nil {
		return nil, err
	}

	return exp.Eval(ds.env)
}

func ParseResultStream(stream string) *ResultStream {
	p := parser.NewParser(strings.NewReader(stream))
	env := environment.New(nil)

	return &ResultStream{p, env}
}

func RequireStreamEq(t *testing.T, raw string, res *genji.Result) {
	// t.Helper()
	docs := ParseResultStream(raw)

	want := document.NewValueBuffer()

	for {
		v, err := docs.Next()
		if err != nil {
			if perr, ok := errors.Unwrap(err).(*parser.ParseError); ok {
				if perr.Found == "EOF" {
					break
				}
			}
		}
		assert.NoError(t, err)

		want.Append(v)
	}

	got := document.NewValueBuffer()

	err := res.Iterate(func(d types.Document) error {
		var fb document.FieldBuffer
		err := fb.Copy(d)
		if err != nil {
			return err
		}
		got.Append(types.NewDocumentValue(&fb))
		return nil
	})
	assert.NoError(t, err)

	expected, err := types.MarshalTextIndent(types.NewArrayValue(want), "\n", "  ")
	assert.NoError(t, err)

	actual, err := types.MarshalTextIndent(types.NewArrayValue(got), "\n", "  ")
	assert.NoError(t, err)
	require.Equal(t, string(expected), string(actual))
}
