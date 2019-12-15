package document_test

import (
	"bytes"
	"testing"

	"github.com/asdine/genji/document"
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
				Add("name", document.NewStringValue("John")).
				Add("age", document.NewUint16Value(10)),
			`{"name":"John","age":10}` + "\n",
		},
		{
			"Nested",
			document.NewFieldBuffer().
				Add("name", document.NewStringValue("John")).
				Add("age", document.NewUint16Value(10)).
				Add("address", document.NewDocumentValue(document.NewFieldBuffer().
					Add("city", document.NewStringValue("Ajaccio")).
					Add("country", document.NewStringValue("France")),
				)).
				Add("friends", document.NewArrayValue(
					document.NewValueBuffer().
						Append(document.NewStringValue("fred")).
						Append(document.NewStringValue("jamie")),
				)),
			`{"name":"John","age":10,"address":{"city":"Ajaccio","country":"France"},"friends":["fred","jamie"]}` + "\n",
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
