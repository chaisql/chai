package testutil

import (
	"fmt"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/stretchr/testify/require"
)

// MakeValue turns v into a document.Value.
func MakeValue(t testing.TB, v interface{}) *document.Value {
	t.Helper()

	vv, err := document.NewValue(v)
	require.NoError(t, err)
	return &vv
}

// MakeDocument creates a document from a json string.
func MakeDocument(jsonDoc string) document.Document {
	return document.NewFromJSON([]byte(jsonDoc))
}

// MakeDocuments creates a slice of document from json strings.
func MakeDocuments(jsonDocs ...string) (docs Docs) {
	for _, jsonDoc := range jsonDocs {
		docs = append(docs, MakeDocument(jsonDoc))
	}
	return
}

type Docs []document.Document

func (docs Docs) RequireEqual(t testing.TB, others Docs) {
	t.Helper()

	require.Equal(t, len(docs), len(others), fmt.Sprintf("expected len %d, got %d", len(docs), len(others)))

	for i, d := range docs {
		l := document.NewDocumentValue(d)
		r := document.NewDocumentValue(others[i])
		ok, err := l.IsEqual(r)
		require.NoError(t, err)
		if !ok {
			require.Equal(t, l, r)
		}
	}
}
