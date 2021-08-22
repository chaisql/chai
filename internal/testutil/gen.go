package testutil

import (
	"strings"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

type ResultStream struct {
	*parser.Parser
	env *environment.Environment
}

func (ds *ResultStream) Next() (*types.Value, error) {
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

func ParseResultStream(stream string) *ResultStream {
	p := parser.NewParser(strings.NewReader(stream))
	env := environment.New(document.NewFieldBuffer())

	return &ResultStream{p, env}
}

// val is a fully realized representation of a types.Value, suitable for
// comparison with go-cmp.
type val struct {
	Type string
	V    interface{}
}

func transformV(v types.Value) val {
	var vi interface{}

	if v.Type() == types.DocumentValue {
		vi = transformDoc(v.V().(types.Document))
	} else if v.Type() == types.ArrayValue {
		vi = transformArray(v.V().(types.Array))
	} else {
		vi = document.ValueToString(v)
	}

	return val{
		Type: v.Type().String(),
		V:    vi,
	}
}

type field struct {
	Field string
	V     val
}

// doc is a fully realized representation of a types.Document, suitable for
// comparison with go-cmp.
type doc []field

func transformDoc(d types.Document) doc {
	fields := make([]field, 0)
	_ = d.Iterate(func(name string, v types.Value) error {
		fields = append(fields, field{Field: name, V: transformV(v)})
		return nil
	})

	return fields
}

func transformArray(a types.Array) []val {
	fields := make([]val, 0)
	_ = a.Iterate(func(i int, v types.Value) error {
		fields = append(fields, transformV(v))
		return nil
	})

	return fields
}

func RequireStreamEq(t *testing.T, raw string, res *genji.Result) {
	t.Helper()
	docs := ParseResultStream(raw)
	var want []*val

	for v, err := docs.Next(); err == nil; v, err = docs.Next() {
		val := transformV(*v)
		want = append(want, &val)
	}

	var got []*val
	err := res.Iterate(func(d types.Document) error {
		val := transformV(types.NewDocumentValue(d))
		got = append(got, &val)
		return nil
	})
	assert.NoError(t, err)

	// Working with simple structs rather than providing an equality function to go-cmp
	// allows to get much better diffs, which only show where the difference is rater than
	// fully displaying both documents.
	diff := cmp.Diff(want, got)
	if diff != "" {
		require.Failf(t, "mismatched documents, (-want, +got)", "%s", diff)
	}
}
