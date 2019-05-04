package query

import (
	"fmt"
	"testing"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	"github.com/stretchr/testify/require"
)

func createTable(t require.TestingT, size int) (*genji.Tx, func()) {
	db, err := genji.New(memory.NewEngine())

	tx, err := db.Begin(true)
	require.NoError(t, err)

	err = tx.CreateTable("test")
	require.NoError(t, err)

	tb, err := tx.Table("test")
	require.NoError(t, err)

	for i := 0; i < size; i++ {
		_, err = tb.Insert(record.FieldBuffer{
			field.NewInt64("id", int64(i)),
			field.NewString("name", fmt.Sprintf("john-%d", i)),
			field.NewInt64("age", int64(i*10)),
			field.NewInt64("group", int64(i%3)),
		})
		require.NoError(t, err)
	}

	return tx, func() {
		tx.Rollback()
		db.Close()
	}
}

func TestQuery(t *testing.T) {
	t.Run("Select", func(t *testing.T) {
		t.Run("Ok", func(t *testing.T) {
			tx, cleanup := createTable(t, 10)
			defer cleanup()

			res := Select(Field("id"), Field("name")).From(Table("test")).Where(GtInt(Field("age"), 20)).Run(tx)
			require.NoError(t, res.Err())

			b := table.NewBrowser(res.Table())
			count, err := b.Count()
			require.NoError(t, err)
			require.Equal(t, 7, count)

			err = table.NewBrowser(res.Table()).ForEach(func(rowid []byte, r record.Record) error {
				_, err := r.Field("id")
				require.NoError(t, err)
				_, err = r.Field("name")
				require.NoError(t, err)
				_, err = r.Field("age")
				require.Error(t, err)
				_, err = r.Field("group")
				require.Error(t, err)

				return nil
			}).Err()
			require.NoError(t, err)
		})
	})
}

func BenchmarkQuery(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%0.5d", size), func(b *testing.B) {
			tx, cleanup := createTable(b, size)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				Select(Field("id"), Field("name"), Field("age"), Field("group")).From(Table("test")).Where(GtInt(Field("age"), -200)).Run(tx)
			}
			b.StopTimer()
			tx.Rollback()
		})

	}

}
