package recordutil_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/asdine/genji/record"
	"github.com/asdine/genji/record/recordutil"
	"github.com/stretchr/testify/require"
)

func TestIteratorToCSV(t *testing.T) {
	tests := []struct {
		name     string
		expected string
	}{
		{"OK", `John 0,10
John 1,11
John 2,12
`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var records []record.Record

			for i := 0; i < 3; i++ {
				records = append(records, record.FieldBuffer([]record.Field{
					record.NewStringField("name", fmt.Sprintf("John %d", i)),
					record.NewIntField("age", 10+i),
				}))
			}

			var buf bytes.Buffer
			err := recordutil.IteratorToCSV(&buf, record.NewStream(record.NewIterator(records...)))
			require.NoError(t, err)
			require.Equal(t, test.expected, buf.String())
			require.NoError(t, err)
		})
	}
}

func TestRecordToJSON(t *testing.T) {
	tests := []struct {
		name     string
		expected string
	}{
		{"OK", `{"name":"John","age":10}` + "\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := record.FieldBuffer([]record.Field{
				record.NewStringField("name", "John"),
				record.NewUint16Field("age", 10),
			})

			var buf bytes.Buffer
			err := recordutil.RecordToJSON(&buf, r)
			require.NoError(t, err)
			require.Equal(t, test.expected, buf.String())
			require.NoError(t, err)
		})
	}
}
