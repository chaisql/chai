package memory

import (
	"testing"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"

	"github.com/stretchr/testify/require"
)

func TestTable(t *testing.T) {
	ng := NewEngine()
	tx, err := ng.Begin(true)
	require.NoError(t, err)

	tr, err := tx.CreateTable("test")
	require.NoError(t, err)

	rec := record.FieldBuffer{
		field.NewString("name", "John"),
		field.NewInt64("age", 10),
	}
	rowid, err := tr.Insert(rec)
	require.NoError(t, err)

	resp, err := tr.Record(rowid)
	require.NoError(t, err)
	require.Equal(t, rec, resp)
}
