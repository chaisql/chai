package sql

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/query"
	"github.com/asdine/genji/record"
	"github.com/stretchr/testify/require"
)

func createTable(t require.TestingT, size int, withIndex bool) (*genji.DB, func()) {
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
			field.NewInt64("age", int64(i*10)),
			field.NewInt("group", int(i%3)),
		})
		time.Sleep(time.Millisecond)
		require.NoError(t, err)
	}

	require.NoError(t, tx.Commit())

	return db, func() {
		db.Close()
	}
}

func TestParser(t *testing.T) {
	db, cleanup := createTable(t, 10, false)
	defer cleanup()

	q, err := ParseQuery("SELECT FROM test WHERE age = 10")
	require.NoError(t, err)

	res := q.Run(db)
	count, err := res.Count()
	require.NoError(t, err)
	require.Equal(t, 1, count)
	_, r, err := res.First()
	require.NoError(t, err)
	record.DumpRecord(os.Stderr, r)
	idf, err := r.GetField("id")
	require.NoError(t, err)
	id, err := field.Decode(idf)
	require.NoError(t, err)
	require.Equal(t, 1, id)
}

func BenchmarkQuery(b *testing.B) {
	db, cleanup := createTable(b, 10000, false)
	defer cleanup()

	b.Run("SQL", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q, _ := ParseQuery("SELECT FROM test WHERE age = 10")

			q.Run(db).Count()
		}
		b.StopTimer()
	})

	b.Run("Naked", func(b *testing.B) {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			q := query.New(query.Select().From(query.Table("test")).Where(query.Eqq(query.Field("age"), query.Scalar{
				Type:  field.Int64,
				Data:  field.EncodeInt64(10),
				Value: 10,
			})))

			q.Run(db).Count()
		}
		b.StopTimer()
	})
}

var q1 query.Query
var q2 query.Query

func BenchmarkParseQuery(b *testing.B) {
	b.Run("SQL", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			q1, _ = ParseQuery("SELECT FROM test WHERE age = 10 AND name = 'hey'")
		}
	})

	b.Run("Naked", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			q2 = query.New(query.Select().From(query.Table("test")).Where(query.Eqq(query.Field("age"), query.Scalar{
				Type:  field.Int64,
				Data:  field.EncodeInt64(10),
				Value: 10,
			})))
		}
	})
}
