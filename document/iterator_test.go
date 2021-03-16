package document_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/stretchr/testify/require"
)

func TestIteratorToJSONArray(t *testing.T) {
	var docs []document.Document
	for i := 0; i < 3; i++ {
		fb := document.NewFieldBuffer()
		err := json.Unmarshal([]byte(fmt.Sprintf(`{"a": %d}`, i)), fb)
		require.NoError(t, err)
		docs = append(docs, fb)
	}

	it := document.NewIterator(docs...)
	var buf bytes.Buffer
	err := document.IteratorToJSONArray(&buf, it)
	require.NoError(t, err)
	require.Equal(t, `[{"a": 0}, {"a": 1}, {"a": 2}]`, buf.String())
}
