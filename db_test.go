package genji_test

import (
	"fmt"
	"testing"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/value"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func newTestDB(t testing.TB) (*genji.Tx, func()) {
	db, err := genji.New(memory.NewEngine())
	require.NoError(t, err)

	tx, err := db.Begin(true)
	require.NoError(t, err)

	return tx, func() {
		tx.Rollback()
	}
}

func newTestTable(t testing.TB) (*genji.Table, func()) {
	tx, fn := newTestDB(t)

	tb, err := tx.CreateTable("test")
	require.NoError(t, err)

	return tb, fn
}

func TestTxCreateIndex(t *testing.T) {
	t.Run("Should create an index and return it", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		_, err := tx.CreateTable("test")
		require.NoError(t, err)

		idx, err := tx.CreateIndex("idxFoo", "test", "foo", index.Options{})
		require.NoError(t, err)
		require.NotNil(t, idx)
	})

	t.Run("Should fail if it already exists", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		_, err := tx.CreateTable("test")
		require.NoError(t, err)

		_, err = tx.CreateIndex("idxFoo", "test", "foo", index.Options{})
		require.NoError(t, err)

		_, err = tx.CreateIndex("idxFoo", "test", "foo", index.Options{})
		require.Equal(t, genji.ErrIndexAlreadyExists, err)
	})

	t.Run("Should fail if table doesn't exists", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		_, err := tx.CreateIndex("idxFoo", "test", "foo", index.Options{})
		require.Equal(t, genji.ErrTableNotFound, err)
	})
}

func TestTxDropIndex(t *testing.T) {
	t.Run("Should drop an index", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		_, err := tx.CreateTable("test")
		require.NoError(t, err)

		_, err = tx.CreateIndex("idxFoo", "test", "foo", index.Options{})
		require.NoError(t, err)

		err = tx.DropIndex("idxFoo")
		require.NoError(t, err)

		_, err = tx.GetIndex("idxFoo")
		require.Error(t, err)
	})

	t.Run("Should fail if it doesn't exist", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.DropIndex("idxFoo")
		require.Equal(t, genji.ErrIndexNotFound, err)
	})
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
		require.Equal(t, genji.ErrRecordNotFound, err)
		require.Nil(t, r)
	})

	t.Run("Should return the right record", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		// create two records, one with an additional field
		rec1 := newRecord()
		rec1.Add(record.NewInt64Field("fieldc", 40))
		rec2 := newRecord()

		key1, err := tb.Insert(rec1)
		require.NoError(t, err)
		_, err = tb.Insert(rec2)
		require.NoError(t, err)

		// fetch rec1 and make sure it returns the right one
		res, err := tb.GetRecord(key1)
		require.NoError(t, err)
		fc, err := res.GetField("fieldc")
		require.NoError(t, err)
		require.Equal(t, rec1[2], fc)
	})
}

// TestTableInsert verifies Insert behaviour.
func TestTableInsert(t *testing.T) {
	t.Run("Should generate a key by default", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		rec := newRecord()
		key1, err := tb.Insert(rec)
		require.NoError(t, err)
		require.NotEmpty(t, key1)

		key2, err := tb.Insert(rec)
		require.NoError(t, err)
		require.NotEmpty(t, key2)

		require.NotEqual(t, key1, key2)
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
		key, err := tb.Insert(rec)
		require.NoError(t, err)
		require.Equal(t, value.EncodeInt64(2), key)

		// make sure the record is fetchable using the returned key
		_, err = tb.GetRecord(key)
		require.NoError(t, err)

		// insert again
		key, err = tb.Insert(rec)
		require.NoError(t, err)
		require.Equal(t, value.EncodeInt64(4), key)
	})

	t.Run("Should fail if Pk returns empty key", func(t *testing.T) {
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

	t.Run("Should return ErrDuplicate if key already exists", func(t *testing.T) {
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
		require.Equal(t, genji.ErrDuplicateRecord, err)
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
		require.Equal(t, genji.ErrRecordNotFound, err)
	})

	t.Run("Should delete the right record", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		// create two records, one with an additional field
		rec1 := newRecord()
		rec1.Add(record.NewInt64Field("fieldc", 40))
		rec2 := newRecord()

		key1, err := tb.Insert(rec1)
		require.NoError(t, err)
		key2, err := tb.Insert(rec2)
		require.NoError(t, err)

		// delete the record
		err = tb.Delete([]byte(key1))
		require.NoError(t, err)

		// try again, should fail
		err = tb.Delete([]byte(key1))
		require.Equal(t, genji.ErrRecordNotFound, err)

		// make sure it didn't also delete the other one
		res, err := tb.GetRecord(key2)
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
		require.Equal(t, genji.ErrRecordNotFound, err)
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

		key1, err := tb.Insert(rec1)
		require.NoError(t, err)
		key2, err := tb.Insert(rec2)
		require.NoError(t, err)

		// create a third record
		rec3 := record.FieldBuffer([]record.Field{
			record.NewStringField("fielda", "e"),
			record.NewStringField("fieldb", "f"),
		})

		// replace rec1 with rec3
		err = tb.Replace(key1, rec3)
		require.NoError(t, err)

		// make sure it replaced it correctly
		res, err := tb.GetRecord(key1)
		require.NoError(t, err)
		f, err := res.GetField("fielda")
		require.NoError(t, err)
		require.Equal(t, "e", string(f.Data))

		// make sure it didn't also replace the other one
		res, err = tb.GetRecord(key2)
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

func TestTableIndexes(t *testing.T) {
	t.Run("Should succeed if table has no indexes", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		m, err := tb.Indexes()
		require.NoError(t, err)
		require.Empty(t, m)
	})

	t.Run("Should return a map of all the indexes", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		tb, err := tx.CreateTable("test1")
		require.NoError(t, err)
		_, err = tx.CreateTable("test2")
		require.NoError(t, err)

		_, err = tx.CreateIndex("idx1a", "test1", "a", index.Options{Unique: true})
		require.NoError(t, err)
		_, err = tx.CreateIndex("idx1b", "test1", "b", index.Options{Unique: false})
		require.NoError(t, err)
		_, err = tx.CreateIndex("ifx2a", "test2", "a", index.Options{Unique: false})
		require.NoError(t, err)

		m, err := tb.Indexes()
		require.NoError(t, err)
		require.Len(t, m, 2)
		idx1a, ok := m["idx1a"]
		require.True(t, ok)
		require.NotNil(t, idx1a)
		idx1b, ok := m["idx1a"]
		require.True(t, ok)
		require.NotNil(t, idx1b)
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
