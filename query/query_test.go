package query_test

import (
	"fmt"
	"testing"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/query"
	"github.com/asdine/genji/query/expr"
	"github.com/asdine/genji/query/q"
	"github.com/asdine/genji/record"
	"github.com/stretchr/testify/require"
)

func createTable(t require.TestingT, size int, withIndex bool) (*database.Tx, func()) {
	db, err := database.New(memory.NewEngine())
	require.NoError(t, err)

	tx, err := db.Begin(true)
	require.NoError(t, err)

	tb, err := tx.CreateTable("test")
	require.NoError(t, err)

	if withIndex {
		_, err = tb.CreateIndex("idx_name", "name", index.Options{})
		require.NoError(t, err)
	}

	for i := 0; i < size; i++ {
		_, err = tb.Insert(record.FieldBuffer{
			record.NewIntField("id", int(i)),
			record.NewStringField("name", fmt.Sprintf("john-%d", i)),
			record.NewIntField("age", int(i*10)),
			record.NewIntField("group", int(i%3)),
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
				query.Select().From(q.Table("test")).Where(q.StringField("name").Eq("john-1")).Exec(tx).Count()
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
				query.Select().From(q.Table("test")).Where(q.IntField("age").Gt(-200)).Limit(size/10 + 1).Exec(tx).Count()
			}
			b.StopTimer()
			tx.Rollback()
		})
	}
}

func BenchmarkStatementSelectWithIndex(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%0.5d", size), func(b *testing.B) {
			tx, cleanup := createTable(b, size, true)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				query.Select().From(q.Table("test")).Where(q.StringField("name").Eq("john-1")).Exec(tx).Count()
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
				res := query.Delete().From("test").Where(q.IntField("age").Gt(-200)).Exec(tx)
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
				res := query.Update("test").Where(q.IntField("age").Gt(-200)).Set("age", expr.IntValue(100)).Exec(tx)
				require.NoError(b, res.Err())
			}
			b.StopTimer()
			tx.Rollback()
		})
	}
}
