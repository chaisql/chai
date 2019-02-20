package table_test

import (
	"testing"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	"github.com/asdine/genji/table/tabletest"
	"github.com/stretchr/testify/require"
)

type recordPker struct {
	record.FieldBuffer
	pkGenerator func() ([]byte, error)
}

func (r recordPker) Pk() ([]byte, error) {
	return r.pkGenerator()
}

func TestRecordBuffer(t *testing.T) {
	tabletest.TestSuite(t, func() (table.Table, func()) {
		var rb table.RecordBuffer
		return &rb, func() {}
	})
}

func TestRecordBufferr(t *testing.T) {
	t.Run("Insert", func(t *testing.T) {
		t.Run("Default autoincrement counter", func(t *testing.T) {
			var r table.RecordBuffer

			rowid, err := r.Insert(new(record.FieldBuffer))
			require.NoError(t, err)
			require.Equal(t, field.EncodeInt64(1), rowid)

			rowid, err = r.Insert(new(record.FieldBuffer))
			require.NoError(t, err)
			require.Equal(t, field.EncodeInt64(2), rowid)
		})

		t.Run("Pker support", func(t *testing.T) {
			var counter int64

			rec := recordPker{
				pkGenerator: func() ([]byte, error) {
					counter += 2
					return field.EncodeInt64(counter), nil
				},
			}

			var r table.RecordBuffer

			rowid, err := r.Insert(rec)
			require.NoError(t, err)
			require.Equal(t, field.EncodeInt64(2), rowid)

			rowid, err = r.Insert(rec)
			require.NoError(t, err)
			require.Equal(t, field.EncodeInt64(4), rowid)
		})
	})
}
