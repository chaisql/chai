package query

import (
	"fmt"
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
	tests := []struct {
		name     string
		s        string
		expected Statement
	}{
		{"=", "SELECT FROM test WHERE age = 10", Select().From(Table("test")).Where(Eq(Field("age"), Int64Value(10)))},
		{"AND", "SELECT FROM test WHERE age = 10 AND age <= 11",
			Select().From(Table("test")).Where(And(
				Eq(Field("age"), Int64Value(10)),
				Lte(Field("age"), Int64Value(11)),
			))},
		{"OR", "SELECT FROM test WHERE age = 10 OR age = 11",
			Select().From(Table("test")).Where(Or(
				Eq(Field("age"), Int64Value(10)),
				Eq(Field("age"), Int64Value(11)),
			))},
		{"AND then OR", "SELECT FROM test WHERE age >= 10 AND age > 11 OR age < 10.4",
			Select().From(Table("test")).Where(Or(
				And(
					Gte(Field("age"), Int64Value(10)),
					Gt(Field("age"), Int64Value(11)),
				),
				Lt(Field("age"), Float64Value(10.4)),
			))},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := ParseQuery(test.s)
			require.NoError(t, err)
			require.Len(t, q.statements, 1)
			require.EqualValues(t, test.expected, q.statements[0])
		})
	}
}
