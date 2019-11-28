package document_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/asdine/genji/document"
	"github.com/stretchr/testify/require"
)

func TestIteratorToCSV(t *testing.T) {
	tests := []struct {
		name     string
		expected string
	}{
		{"OK", `"John, 0",10,3.14,NULL
"John, 1",11,6.28,NULL
"John, 2",12,9.42,NULL
`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var records []document.Record

			for i := 0; i < 3; i++ {
				records = append(records, document.FieldBuffer([]document.Field{
					document.NewStringField("name", fmt.Sprintf("John, %d", i)),
					document.NewIntField("age", 10+i),
					document.NewFloat64Field("pi", 3.14*float64(i+1)),
					document.NewNullField("friends"),
				}))
			}

			var buf bytes.Buffer
			err := document.IteratorToCSV(&buf, document.NewStream(document.NewIterator(records...)))
			require.NoError(t, err)
			require.Equal(t, test.expected, buf.String())
			require.NoError(t, err)
		})
	}
}
