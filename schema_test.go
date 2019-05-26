package genji

import (
	"testing"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/stretchr/testify/require"
)

func TestSchemaRecord(t *testing.T) {
	s := record.Schema{
		Fields: []field.Field{
			field.NewInt64("a", 0),
			field.NewString("b", ""),
		},
	}

	sr := schemaRecord{
		TableName: "table",
		Schema:    &s,
	}

	data, err := record.Encode(&sr)
	require.NoError(t, err)

	er := record.EncodedRecord(data)
	var expected schemaRecord
	err = expected.ScanRecord(er)
	require.NoError(t, err)

	require.Equal(t, sr, expected)
}
