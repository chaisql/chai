package testutil

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

// MakeValue turns v into a types.Value.
func MakeValue(t testing.TB, v interface{}) types.Value {
	t.Helper()

	vv, err := document.NewValue(v)
	assert.NoError(t, err)
	return vv
}

func MakeArrayValue(t testing.TB, vs ...interface{}) types.Value {
	t.Helper()

	vvs := []types.Value{}
	for _, v := range vs {
		vvs = append(vvs, MakeValue(t, v))
	}

	vb := document.NewValueBuffer(vvs...)

	return types.NewArrayValue(vb)
}

// MakeDocument creates a document from a json string.
func MakeDocument(t testing.TB, jsonDoc string) types.Document {
	t.Helper()

	var fb document.FieldBuffer

	err := fb.UnmarshalJSON([]byte(jsonDoc))
	assert.NoError(t, err)

	return &fb
}

// MakeDocuments creates a slice of document from json strings.
func MakeDocuments(t testing.TB, jsonDocs ...string) (docs Docs) {
	for _, jsonDoc := range jsonDocs {
		docs = append(docs, MakeDocument(t, jsonDoc))
	}
	return
}

// MakeArray creates an array from a json string.
func MakeArray(t testing.TB, jsonArray string) types.Array {
	t.Helper()

	var vb document.ValueBuffer

	err := vb.UnmarshalJSON([]byte(jsonArray))
	assert.NoError(t, err)

	return &vb
}

func MakeValueBuffer(t testing.TB, jsonArray string) *document.ValueBuffer {
	t.Helper()

	var vb document.ValueBuffer

	err := vb.UnmarshalJSON([]byte(jsonArray))
	assert.NoError(t, err)

	return &vb
}

type Docs []types.Document

func (docs Docs) RequireEqual(t testing.TB, others Docs) {
	t.Helper()

	require.Equal(t, len(docs), len(others), fmt.Sprintf("expected len %d, got %d", len(docs), len(others)))

	for i, d := range docs {
		RequireDocEqual(t, d, others[i])
	}
}

// Dump a json representation of v to os.Stdout.
func Dump(t testing.TB, v interface{}) {
	t.Helper()

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	err := enc.Encode(v)
	assert.NoError(t, err)
}

func RequireDocJSONEq(t testing.TB, d types.Document, expected string) {
	t.Helper()

	data, err := json.Marshal(d)
	assert.NoError(t, err)
	require.JSONEq(t, expected, string(data))
}

// IteratorToJSONArray encodes all the documents of an iterator to a JSON array.
func IteratorToJSONArray(w io.Writer, s document.Iterator) error {
	buf := bufio.NewWriter(w)

	buf.WriteByte('[')

	first := true
	err := s.Iterate(func(d types.Document) error {
		if !first {
			buf.WriteString(", ")
		} else {
			first = false
		}

		data, err := document.MarshalJSON(d)
		if err != nil {
			return err
		}

		_, err = buf.Write(data)
		return err
	})
	if err != nil {
		return err
	}

	buf.WriteByte(']')
	return buf.Flush()
}

func RequireDocEqual(t testing.TB, d1, d2 types.Document) {
	t.Helper()

	t1, err := types.MarshalTextIndent(types.NewDocumentValue(d1), "\n", "  ")
	require.NoError(t, err)
	t2, err := types.MarshalTextIndent(types.NewDocumentValue(d2), "\n", "  ")
	require.NoError(t, err)

	if diff := cmp.Diff(string(t1), string(t2)); diff != "" {
		require.Failf(t, "mismatched documents, (-want, +got)", "%s", diff)
	}
}

func CloneDocument(t testing.TB, d types.Document) *document.FieldBuffer {
	t.Helper()

	var newFb document.FieldBuffer

	err := newFb.Copy(d)
	assert.NoError(t, err)

	return &newFb
}
