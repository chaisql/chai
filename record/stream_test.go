package record_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/asdine/genji/record"
	"github.com/stretchr/testify/require"
)

func TestDump(t *testing.T) {
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
			err := record.NewStream(record.NewIterator(records...)).Dump(&buf)
			fmt.Println(buf.String())
			require.NoError(t, err)
			require.Equal(t, test.expected, buf.String())
			require.NoError(t, err)
		})
	}
}
