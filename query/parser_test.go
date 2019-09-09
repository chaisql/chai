package query

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/record"
	"github.com/stretchr/testify/require"
)

func createDB(t require.TestingT, size int, withIndex bool) (*genji.DB, func()) {
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
	db, cleanup := createDB(t, 10, false)
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
