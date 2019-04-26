package record

import (
	"testing"

	"github.com/asdine/genji/field"
	"github.com/stretchr/testify/require"
)

func TestSchemaRecord(t *testing.T) {
	s := Schema{
		Fields: []field.Field{
			field.NewInt64("a", 0),
			field.NewString("b", ""),
		},
	}

	sr := SchemaRecord{
		TableName: "table",
		Schema:    &s,
	}

	data, err := Encode(&sr)
	require.NoError(t, err)

	er := EncodedRecord(data)
	var expected SchemaRecord
	err = expected.ScanRecord(er)
	require.NoError(t, err)

	require.Equal(t, sr, expected)
}
