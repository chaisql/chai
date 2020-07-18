package document_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/stretchr/testify/require"
)

func TestToJSON(t *testing.T) {
	tests := []struct {
		name     string
		r        document.Document
		expected string
	}{
		{
			"Flat",
			document.NewFieldBuffer().
				Add("name", document.NewTextValue("John")).
				Add("age", document.NewIntegerValue(10)).
				Add(`"something with" quotes`, document.NewIntegerValue(10)),
			`{"name": "John", "age": 10, "\"something with\" quotes": 10}`,
		},
		{
			"Nested",
			document.NewFieldBuffer().
				Add("name", document.NewTextValue("John")).
				Add("age", document.NewIntegerValue(10)).
				Add("address", document.NewDocumentValue(document.NewFieldBuffer().
					Add("city", document.NewTextValue("Ajaccio")).
					Add("country", document.NewTextValue("France")),
				)).
				Add("friends", document.NewArrayValue(
					document.NewValueBuffer().
						Append(document.NewTextValue("fred")).
						Append(document.NewTextValue("jamie")),
				)),
			`{"name": "John", "age": 10, "address": {"city": "Ajaccio", "country": "France"}, "friends": ["fred", "jamie"]}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := document.ToJSON(&buf, test.r)
			require.NoError(t, err)
			require.Equal(t, test.expected, buf.String())
			require.NoError(t, err)
		})
	}
}

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
