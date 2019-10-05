package database_test

import (
	"fmt"
	"testing"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/value"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func newTestTable(t testing.TB) (*database.Table, func()) {
	db, err := database.New(memory.NewEngine())
	require.NoError(t, err)

	tx, err := db.Begin(true)
	require.NoError(t, err)

	tb, err := tx.CreateTable("test")
	require.NoError(t, err)

	return tb, func() {
		tx.Rollback()
	}
}

func newRecord() record.FieldBuffer {
	return record.FieldBuffer([]record.Field{
		record.NewStringField("fielda", "a"),
		record.NewStringField("fieldb", "b"),
	})
}

// TestTableIterate verifies Iterate behaviour.
func TestTableIterate(t *testing.T) {
	t.Run("Should not fail with no records", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		i := 0
		err := tb.Iterate(func(r record.Record) error {
			i++
			return nil
		})
		require.NoError(t, err)
		require.Zero(t, i)
	})

	t.Run("Should iterate over all records", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		for i := 0; i < 10; i++ {
			_, err := tb.Insert(newRecord())
			require.NoError(t, err)
		}

		m := make(map[string]int)
		err := tb.Iterate(func(r record.Record) error {
			m[string(r.(record.Keyer).Key())]++
			return nil
		})
		require.NoError(t, err)
		require.Len(t, m, 10)
		for _, c := range m {
			require.Equal(t, 1, c)
		}
	})

	t.Run("Should stop if fn returns error", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		for i := 0; i < 10; i++ {
			_, err := tb.Insert(newRecord())
			require.NoError(t, err)
		}

		i := 0
		err := tb.Iterate(func(_ record.Record) error {
			i++
			if i >= 5 {
				return errors.New("some error")
			}
			return nil
		})
		require.EqualError(t, err, "some error")
		require.Equal(t, 5, i)
	})
}

// TestTableRecord verifies Record behaviour.
func TestTableRecord(t *testing.T) {
	t.Run("Should fail if not found", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		r, err := tb.GetRecord([]byte("id"))
		require.Equal(t, database.ErrRecordNotFound, err)
		require.Nil(t, r)
	})

	t.Run("Should return the right record", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		// create two records, one with an additional field
		rec1 := newRecord()
		rec1.Add(record.NewInt64Field("fieldc", 40))
		rec2 := newRecord()

		recordID1, err := tb.Insert(rec1)
		require.NoError(t, err)
		_, err = tb.Insert(rec2)
		require.NoError(t, err)

		// fetch rec1 and make sure it returns the right one
		res, err := tb.GetRecord(recordID1)
		require.NoError(t, err)
		fc, err := res.GetField("fieldc")
		require.NoError(t, err)
		require.Equal(t, rec1[2], fc)
	})
}

// TestTableInsert verifies Insert behaviour.
func TestTableInsert(t *testing.T) {
	t.Run("Should generate a recordID by default", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		rec := newRecord()
		recordID1, err := tb.Insert(rec)
		require.NoError(t, err)
		require.NotEmpty(t, recordID1)

		recordID2, err := tb.Insert(rec)
		require.NoError(t, err)
		require.NotEmpty(t, recordID2)

		require.NotEqual(t, recordID1, recordID2)
	})

	t.Run("Should support PrimaryKeyer interface", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		var counter int64

		rec := recordPker{
			pkGenerator: func() ([]byte, error) {
				counter += 2
				return value.EncodeInt64(counter), nil
			},
		}

		// insert
		recordID, err := tb.Insert(rec)
		require.NoError(t, err)
		require.Equal(t, value.EncodeInt64(2), recordID)

		// make sure the record is fetchable using the returned recordID
		_, err = tb.GetRecord(recordID)
		require.NoError(t, err)

		// insert again
		recordID, err = tb.Insert(rec)
		require.NoError(t, err)
		require.Equal(t, value.EncodeInt64(4), recordID)
	})

	t.Run("Should fail if Pk returns empty recordID", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		tests := [][]byte{
			nil,
			[]byte{},
			[]byte(nil),
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("%#v", test), func(t *testing.T) {
				rec := recordPker{
					pkGenerator: func() ([]byte, error) {
						return test, nil
					},
				}

				_, err := tb.Insert(rec)
				require.Error(t, err)
			})
		}
	})

	t.Run("Should return ErrDuplicate if recordID already exists", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		rec := recordPker{
			pkGenerator: func() ([]byte, error) {
				return value.EncodeInt64(1), nil
			},
		}

		// insert
		_, err := tb.Insert(rec)
		require.NoError(t, err)

		_, err = tb.Insert(rec)
		require.Equal(t, database.ErrDuplicateRecord, err)
	})
}

type recordPker struct {
	record.FieldBuffer
	pkGenerator func() ([]byte, error)
}

func (r recordPker) PrimaryKey() ([]byte, error) {
	return r.pkGenerator()
}

// TestTableDelete verifies Delete behaviour.
func TestTableDelete(t *testing.T) {
	t.Run("Should fail if not found", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		err := tb.Delete([]byte("id"))
		require.Equal(t, database.ErrRecordNotFound, err)
	})

	t.Run("Should delete the right record", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		// create two records, one with an additional field
		rec1 := newRecord()
		rec1.Add(record.NewInt64Field("fieldc", 40))
		rec2 := newRecord()

		recordID1, err := tb.Insert(rec1)
		require.NoError(t, err)
		recordID2, err := tb.Insert(rec2)
		require.NoError(t, err)

		// delete the record
		err = tb.Delete([]byte(recordID1))
		require.NoError(t, err)

		// try again, should fail
		err = tb.Delete([]byte(recordID1))
		require.Equal(t, database.ErrRecordNotFound, err)

		// make sure it didn't also delete the other one
		res, err := tb.GetRecord(recordID2)
		require.NoError(t, err)
		_, err = res.GetField("fieldc")
		require.Error(t, err)
	})
}

// TestTableReplace verifies Replace behaviour.
func TestTableReplace(t *testing.T) {
	t.Run("Should fail if not found", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		err := tb.Replace([]byte("id"), newRecord())
		require.Equal(t, database.ErrRecordNotFound, err)
	})

	t.Run("Should replace the right record", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		// create two different records
		rec1 := newRecord()
		rec2 := record.FieldBuffer([]record.Field{
			record.NewStringField("fielda", "c"),
			record.NewStringField("fieldb", "d"),
		})

		recordID1, err := tb.Insert(rec1)
		require.NoError(t, err)
		recordID2, err := tb.Insert(rec2)
		require.NoError(t, err)

		// create a third record
		rec3 := record.FieldBuffer([]record.Field{
			record.NewStringField("fielda", "e"),
			record.NewStringField("fieldb", "f"),
		})

		// replace rec1 with rec3
		err = tb.Replace(recordID1, rec3)
		require.NoError(t, err)

		// make sure it replaced it correctly
		res, err := tb.GetRecord(recordID1)
		require.NoError(t, err)
		f, err := res.GetField("fielda")
		require.NoError(t, err)
		require.Equal(t, "e", string(f.Data))

		// make sure it didn't also replace the other one
		res, err = tb.GetRecord(recordID2)
		require.NoError(t, err)
		f, err = res.GetField("fielda")
		require.NoError(t, err)
		require.Equal(t, "c", string(f.Data))
	})
}

// TestTableTruncate verifies Truncate behaviour.
func TestTableTruncate(t *testing.T) {
	t.Run("Should succeed if table empty", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		err := tb.Truncate()
		require.NoError(t, err)
	})

	t.Run("Should truncate the table", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		// create two records
		rec1 := newRecord()
		rec2 := newRecord()

		_, err := tb.Insert(rec1)
		require.NoError(t, err)
		_, err = tb.Insert(rec2)
		require.NoError(t, err)

		err = tb.Truncate()
		require.NoError(t, err)

		err = tb.Iterate(func(_ record.Record) error {
			return errors.New("should not iterate")
		})

		require.NoError(t, err)
	})
}

func TestTableCreateIndex(t *testing.T) {
	t.Run("Should create an index and return it", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		idx, err := tb.CreateIndex("idxFoo", "foo", index.Options{})
		require.NoError(t, err)
		require.NotNil(t, idx)
	})

	t.Run("Should fail it already exists", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		_, err := tb.CreateIndex("idxFoo", "foo", index.Options{})
		require.NoError(t, err)

		_, err = tb.CreateIndex("idxFoo", "foo", index.Options{})
		require.Equal(t, database.ErrIndexAlreadyExists, err)
	})
}

func TestTableCreateIndexIfNotExists(t *testing.T) {
	t.Run("Should create an index and return it", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		idx, err := tb.CreateIndexIfNotExists("idxFoo", "foo", index.Options{})
		require.NoError(t, err)
		require.NotNil(t, idx)
	})

	t.Run("Should success if it already exists", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		idx, err := tb.CreateIndex("idxFoo", "foo", index.Options{})
		require.NoError(t, err)

		idx2, err := tb.CreateIndexIfNotExists("idxFoo", "foo", index.Options{})
		require.NoError(t, err)
		require.Equal(t, idx, idx2)
	})
}

// BenchmarkTableInsert benchmarks the Insert method with 1, 10, 1000 and 10000 successive insertions.
func BenchmarkTableInsert(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			var fields []record.Field

			for i := int64(0); i < 10; i++ {
				fields = append(fields, record.NewInt64Field(fmt.Sprintf("name-%d", i), i))
			}

			rec := record.FieldBuffer(fields)

			b.ResetTimer()
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				tb, cleanup := newTestTable(b)

				b.StartTimer()
				for j := 0; j < size; j++ {
					tb.Insert(rec)
				}
				b.StopTimer()
				cleanup()
			}
		})
	}
}

// BenchmarkTableScan benchmarks the Scan method with 1, 10, 1000 and 10000 successive insertions.
func BenchmarkTableScan(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			tb, cleanup := newTestTable(b)
			defer cleanup()

			var fields []record.Field

			for i := int64(0); i < 10; i++ {
				fields = append(fields, record.NewInt64Field(fmt.Sprintf("name-%d", i), i))
			}

			rec := record.FieldBuffer(fields)

			for i := 0; i < size; i++ {
				_, err := tb.Insert(rec)
				require.NoError(b, err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tb.Iterate(func(record.Record) error {
					return nil
				})
			}
			b.StopTimer()
		})
	}
}
