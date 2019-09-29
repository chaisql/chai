package query

import (
	"fmt"
	"testing"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/record"
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

func BenchmarkStatementSelect(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%0.5d", size), func(b *testing.B) {
			tx, cleanup := createTable(b, size, false)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				Select().From(Table("test")).Where(IntField("age").Gt(-200)).Exec(tx).Count()
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
				Select().From(Table("test")).Where(IntField("age").Gt(-200)).Limit(size/10 + 1).Exec(tx).Count()
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
				Select().From(Table("test")).Where(StringField("name").Gt("")).Exec(tx).Count()
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
				res := Delete().From(Table("test")).Where(IntField("age").Gt(-200)).Exec(tx)
				require.NoError(b, res.Err())
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
				res := Update(Table("test")).Where(IntField("age").Gt(-200)).Set("age", IntValue(100)).Exec(tx)
				require.NoError(b, res.Err())
			}
			b.StopTimer()
			tx.Rollback()
		})
	}
}
