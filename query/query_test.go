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

func createTable(t require.TestingT, size int, withIndex bool) (*genji.Tx, func()) {
	db := genji.New(memory.NewEngine())

	tx, err := db.Begin(true)
	require.NoError(t, err)

	err = tx.CreateTable("test")
	require.NoError(t, err)

	tb, err := tx.Table("test")
	require.NoError(t, err)

	if withIndex {
		err = tx.CreateIndex("test", "name")
		require.NoError(t, err)
	}

	for i := 0; i < size; i++ {
		_, err = tb.Insert(record.FieldBuffer{
			field.NewInt("id", int(i)),
			field.NewString("name", fmt.Sprintf("john-%d", i)),
			field.NewInt("age", int(i*10)),
			field.NewInt("group", int(i%3)),
		})
		require.NoError(t, err)
	}

	return tx, func() {
		tx.Rollback()
	}
}

func TestSelect(t *testing.T) {
	t.Run("NoIndex", func(t *testing.T) {
		tx, cleanup := createTable(t, 10, false)
		defer cleanup()

		res := Select(Field("id"), Field("name")).From(Table("test")).Where(GtInt(Field("age"), 20)).Limit(5).Offset(1).Run(tx)
		require.NoError(t, res.Err())

		b := table.NewBrowser(res.Table())
		count, err := b.Count()
		require.NoError(t, err)
		require.Equal(t, 5, count)

		err = table.NewBrowser(res.Table()).ForEach(func(recordID []byte, r record.Record) error {
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

	t.Run("WithIndex", func(t *testing.T) {
		tx, cleanup := createTable(t, 10, true)
		defer cleanup()

		res := Select(Field("id"), Field("name")).From(Table("test")).Where(GtString(Field("name"), "john")).Limit(5).Offset(1).Run(tx)
		require.NoError(t, res.Err())

		b := table.NewBrowser(res.Table())
		count, err := b.Count()
		require.NoError(t, err)
		require.Equal(t, 5, count)

		err = table.NewBrowser(res.Table()).ForEach(func(recordID []byte, r record.Record) error {
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
}

func TestDelete(t *testing.T) {
	t.Run("NoIndex", func(t *testing.T) {
		tx, cleanup := createTable(t, 10, false)
		defer cleanup()

		err := Delete().From(Table("test")).Where(GtInt(Field("age"), 20)).Run(tx)
		require.NoError(t, err)

		tb, err := tx.Table("test")
		require.NoError(t, err)

		b := table.NewBrowser(tb)
		count, err := b.Count()
		require.NoError(t, err)
		require.Equal(t, 3, count)

		err = b.ForEach(func(recordID []byte, r record.Record) error {
			f, err := r.Field("age")
			require.NoError(t, err)
			age, err := field.DecodeInt(f.Data)
			require.NoError(t, err)
			require.True(t, age <= 20)
			return nil
		}).Err()
		require.NoError(t, err)
	})
}

func TestInsert(t *testing.T) {
	t.Run("NoFields", func(t *testing.T) {
		tx, cleanup := createTable(t, 10, false)
		defer cleanup()

		res := Insert().Into(Table("test")).Values(IntValue(5), StringValue("hello"), IntValue(50), IntValue(5)).Run(tx)
		require.Error(t, res.Err())
	})

	t.Run("WithFields", func(t *testing.T) {
		tx, cleanup := createTable(t, 10, false)
		defer cleanup()

		res := Insert().Into(Table("test")).Fields("a", "b").Values(IntValue(5), StringValue("hello")).Run(tx)
		require.NoError(t, res.Err())

		tb, err := tx.Table("test")
		require.NoError(t, err)

		b := table.NewBrowser(tb)
		count, err := b.Count()
		require.NoError(t, err)
		require.Equal(t, 11, count)

		rec, err := table.NewBrowser(res.Table()).First()
		require.NoError(t, err)

		recordID, err := rec.Field("recordID")
		require.NoError(t, err)

		rec, err = tb.Record(recordID.Data)
		require.NoError(t, err)
		expected := record.FieldBuffer([]field.Field{
			field.NewInt("a", 5),
			field.NewString("b", "hello"),
		})
		require.EqualValues(t, &expected, rec)
	})
}

func TestUpdate(t *testing.T) {
	tests := []struct {
		name      string
		withIndex bool
	}{
		{"index", true},
		{"noindex", false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tx, cleanup := createTable(t, 10, test.withIndex)
			defer cleanup()

			err := Update(Table("test")).Set("age", IntValue(20)).Where(GtInt(Field("age"), 20)).Run(tx)
			require.NoError(t, err)

			tb, err := tx.Table("test")
			require.NoError(t, err)

			b := table.NewBrowser(tb)
			count, err := b.Count()
			require.NoError(t, err)
			require.Equal(t, 10, count)

			err = b.ForEach(func(recordID []byte, r record.Record) error {
				f, err := r.Field("age")
				require.NoError(t, err)
				age, err := field.DecodeInt(f.Data)
				require.NoError(t, err)
				require.True(t, age <= 20)
				return nil
			}).Err()
			require.NoError(t, err)
		})

	}

}

func BenchmarkSelect(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%0.5d", size), func(b *testing.B) {
			tx, cleanup := createTable(b, size, false)
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

func BenchmarkSelectLimit(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%0.5d", size), func(b *testing.B) {
			tx, cleanup := createTable(b, size, false)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				Select(Field("id"), Field("name"), Field("age"), Field("group")).From(Table("test")).Where(GtInt(Field("age"), -200)).Limit(size/10 + 1).Run(tx)
			}
			b.StopTimer()
			tx.Rollback()
		})
	}
}
