package bolt

import (
	"fmt"
	"path"
	"testing"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	"github.com/asdine/genji/table/tabletest"
	"github.com/stretchr/testify/require"
)

func TestBoltEngineTable(t *testing.T) {
	tabletest.TestSuite(t, func() (table.Table, func()) {
		dir, cleanup := tempDir(t)
		ng, err := NewEngine(path.Join(dir, "test.db"), 0600, nil)
		require.NoError(t, err)

		tx, err := ng.Begin(true)
		require.NoError(t, err)

		tb, err := tx.CreateTable("test")
		require.NoError(t, err)

		return tb, func() {
			tx.Rollback()
			ng.Close()
			cleanup()
		}
	})
}

func TestTableInsert(t *testing.T) {
	b, cleanup := tempBucket(t, true)
	defer cleanup()

	table := Table{Bucket: b}
	rowid, err := table.Insert(record.FieldBuffer([]field.Field{
		field.NewInt64("a", 10),
	}))
	require.NoError(t, err)
	require.NotEmpty(t, rowid)
}

func TestTableIterate(t *testing.T) {
	b, cleanup := tempBucket(t, true)
	defer cleanup()

	table := Table{Bucket: b}
	for i := 0; i < 10; i++ {
		_, err := table.Insert(record.FieldBuffer([]field.Field{
			field.NewString("name", fmt.Sprintf("name-%d", i)),
			field.NewInt64("age", int64(i)),
		}))
		require.NoError(t, err)
	}

	i := 0
	table.Iterate(func(rowid []byte, r record.Record) bool {
		rc := r.Cursor()
		for rc.Next() {
			require.NoError(t, rc.Err())
			f := rc.Field()

			switch f.Name {
			case "name":
				require.Equal(t, fmt.Sprintf("name-%d", i), string(f.Data))
			case "age":
				age, err := field.DecodeInt64(f.Data)
				require.NoError(t, err)
				require.EqualValues(t, i, age)
			}
		}
		i++
		return true
	})
}
