package query

import (
	"fmt"
	"testing"

	"github.com/asdine/genji/index"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	"github.com/stretchr/testify/require"
)

func createTable(t require.TestingT, size int, withIndex bool) (*genji.Tx, func()) {
	db, err := genji.New(memory.NewEngine())
	require.NoError(t, err)

	tx, err := db.Begin(true)
	require.NoError(t, err)

	tb, err := tx.CreateTable("test")
	require.NoError(t, err)

	if withIndex {
		_, err = tb.CreateIndex("name", index.Options{})
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

		res := Select().From(Table("test")).Where(IntField("age").Gt(20)).Limit(5).Offset(1).Run(tx)
		require.NoError(t, res.Err())

		count, err := res.Count()
		require.NoError(t, err)
		require.Equal(t, 5, count)

		err = res.Iterate(func(recordID []byte, r record.Record) error {
			_, err := r.GetField("id")
			require.NoError(t, err)
			_, err = r.GetField("name")
			require.NoError(t, err)
			_, err = r.GetField("age")
			require.NoError(t, err)
			_, err = r.GetField("group")
			require.NoError(t, err)

			return nil
		})
		require.NoError(t, err)
	})

	t.Run("WithIndex", func(t *testing.T) {
		tx, cleanup := createTable(t, 10, true)
		defer cleanup()

		res := Select().From(Table("test")).Where(StringField("name").Gt("john")).Limit(5).Offset(1).Run(tx)
		require.NoError(t, res.Err())

		count, err := res.Count()
		require.NoError(t, err)
		require.Equal(t, 5, count)

		err = res.Iterate(func(recordID []byte, r record.Record) error {
			_, err := r.GetField("id")
			require.NoError(t, err)
			_, err = r.GetField("name")
			require.NoError(t, err)
			_, err = r.GetField("age")
			require.NoError(t, err)
			_, err = r.GetField("group")
			require.NoError(t, err)

			return nil
		})
		require.NoError(t, err)
	})

	t.Run("WithEmptyIndex", func(t *testing.T) {
		db, err := genji.New(memory.NewEngine())
		require.NoError(t, err)

		var n int
		err = db.Update(func(tx *genji.Tx) error {
			tb, err := tx.CreateTable("test")
			if err != nil {
				return err
			}

			_, err = tb.CreateIndex("a", index.Options{})
			if err != nil {
				return err
			}

			res := Select().From(tb).Where(And(StringField("a").Eq("foo"))).Limit(1).Run(tx)
			if res.Err() != nil {
				return res.Err()
			}

			n, err = res.Count()
			return err
		})
		require.NoError(t, err)
		require.Equal(t, 0, n)
	})
}

func TestDelete(t *testing.T) {
	t.Run("NoIndex", func(t *testing.T) {
		tx, cleanup := createTable(t, 10, false)
		defer cleanup()

		err := Delete().From(Table("test")).Where(IntField("age").Gt(20)).Run(tx)
		require.NoError(t, err)

		tb, err := tx.GetTable("test")
		require.NoError(t, err)

		st := table.NewStream(tb)
		count, err := st.Count()
		require.NoError(t, err)
		require.Equal(t, 3, count)

		err = st.Iterate(func(recordID []byte, r record.Record) error {
			f, err := r.GetField("age")
			require.NoError(t, err)
			age, err := field.DecodeInt(f.Data)
			require.NoError(t, err)
			require.True(t, age <= 20)
			return nil
		})
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

		tb, err := tx.GetTable("test")
		require.NoError(t, err)

		st := table.NewStream(tb)
		count, err := st.Count()
		require.NoError(t, err)
		require.Equal(t, 11, count)

		_, rec, err := res.First()
		require.NoError(t, err)

		rIDf, err := rec.GetField("recordID")
		require.NoError(t, err)

		rec, err = tb.GetRecord(rIDf.Data)
		require.NoError(t, err)
		expected := record.FieldBuffer([]field.Field{
			field.NewInt("a", 5),
			field.NewString("b", "hello"),
		})
		d, err := record.Encode(expected)
		require.NoError(t, err)

		require.EqualValues(t, d, []byte(rec.(record.EncodedRecord)))
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

			err := Update(Table("test")).Set("age", IntValue(20)).Where(IntField("age").Gt(20)).Run(tx)
			require.NoError(t, err)

			tb, err := tx.GetTable("test")
			require.NoError(t, err)

			st := table.NewStream(tb)
			count, err := st.Count()
			require.NoError(t, err)
			require.Equal(t, 10, count)

			err = st.Iterate(func(recordID []byte, r record.Record) error {
				f, err := r.GetField("age")
				require.NoError(t, err)
				age, err := field.DecodeInt(f.Data)
				require.NoError(t, err)
				require.True(t, age <= 20)
				return nil
			})
			require.NoError(t, err)
		})
	}
}

func BenchmarkStatementSelect(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%0.5d", size), func(b *testing.B) {
			tx, cleanup := createTable(b, size, false)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				Select().From(Table("test")).Where(IntField("age").Gt(-200)).Run(tx).Count()
			}
			b.StopTimer()
			tx.Rollback()
		})
	}
}

func BenchmarkStatementSelectLimit(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%0.5d", size), func(b *testing.B) {
			tx, cleanup := createTable(b, size, false)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				Select().From(Table("test")).Where(IntField("age").Gt(-200)).Limit(size/10 + 1).Run(tx).Count()
			}
			b.StopTimer()
			tx.Rollback()
		})
	}
}

func BenchmarkStatementSelectWithIndex(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%0.5d", size), func(b *testing.B) {
			tx, cleanup := createTable(b, size, false)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				Select().From(Table("test")).Where(StringField("name").Gt("")).Run(tx).Count()
			}
			b.StopTimer()
			tx.Rollback()
		})
	}
}

func BenchmarkStatementDelete(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%0.5d", size), func(b *testing.B) {
			tx, cleanup := createTable(b, size, false)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := Delete().From(Table("test")).Where(IntField("age").Gt(-200)).Run(tx)
				require.NoError(b, err)
			}
			b.StopTimer()
			tx.Rollback()
		})
	}
}

func BenchmarkStatementUpdate(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%0.5d", size), func(b *testing.B) {
			tx, cleanup := createTable(b, size, false)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := Update(Table("test")).Where(IntField("age").Gt(-200)).Set("age", IntValue(100)).Run(tx)
				require.NoError(b, err)
			}
			b.StopTimer()
			tx.Rollback()
		})
	}
}
