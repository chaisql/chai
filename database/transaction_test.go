package database_test

import (
	"fmt"
	"testing"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/engine/memoryengine"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/value"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func newTestDB(t testing.TB) (*database.Transaction, func()) {
	db, err := database.New(memoryengine.NewEngine())
	require.NoError(t, err)

	tx, err := db.Begin(true)
	require.NoError(t, err)

	return tx, func() {
		tx.Rollback()
	}
}

func newTestTable(t testing.TB) (*database.Table, func()) {
	tx, fn := newTestDB(t)

	err := tx.CreateTable("test", nil)
	require.NoError(t, err)
	tb, err := tx.GetTable("test")
	require.NoError(t, err)

	return tb, fn
}

func TestTxCreateIndex(t *testing.T) {
	t.Run("Should create an index and return it", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateTable("test", nil)
		require.NoError(t, err)

		err = tx.CreateIndex(index.Options{
			IndexName: "idxFoo", TableName: "test", FieldName: "foo",
		})
		require.NoError(t, err)
		idx, err := tx.GetIndex("idxFoo")
		require.NoError(t, err)
		require.NotNil(t, idx)
	})

	t.Run("Should fail if it already exists", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateTable("test", nil)
		require.NoError(t, err)

		err = tx.CreateIndex(index.Options{
			IndexName: "idxFoo", TableName: "test", FieldName: "foo",
		})
		require.NoError(t, err)

		err = tx.CreateIndex(index.Options{
			IndexName: "idxFoo", TableName: "test", FieldName: "foo",
		})
		require.Equal(t, database.ErrIndexAlreadyExists, err)
	})

	t.Run("Should fail if table doesn't exists", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateIndex(index.Options{
			IndexName: "idxFoo", TableName: "test", FieldName: "foo",
		})
		require.Equal(t, database.ErrTableNotFound, err)
	})
}

func TestTxDropIndex(t *testing.T) {
	t.Run("Should drop an index", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateTable("test", nil)
		require.NoError(t, err)

		err = tx.CreateIndex(index.Options{
			IndexName: "idxFoo", TableName: "test", FieldName: "foo",
		})
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
		require.Equal(t, database.ErrIndexNotFound, err)
	})
}

func TestTxReIndex(t *testing.T) {
	newTestTableFn := func(t *testing.T) (*database.Transaction, *database.Table, func()) {
		tx, cleanup := newTestDB(t)
		err := tx.CreateTable("test", nil)
		require.NoError(t, err)
		tb, err := tx.GetTable("test")
		require.NoError(t, err)

		for i := 0; i < 10; i++ {
			_, err = tb.Insert(document.NewFieldBuffer(
				document.NewIntField("a", i),
				document.NewIntField("b", i*10),
			))
			require.NoError(t, err)
		}

		err = tx.CreateIndex(index.Options{
			IndexName: "a",
			TableName: "test",
			FieldName: "a",
		})
		require.NoError(t, err)
		err = tx.CreateIndex(index.Options{
			IndexName: "b",
			TableName: "test",
			FieldName: "b",
		})
		require.NoError(t, err)

		return tx, tb, cleanup
	}

	t.Run("Should fail if not found", func(t *testing.T) {
		tx, _, cleanup := newTestTableFn(t)
		defer cleanup()

		err := tx.ReIndex("foo")
		require.Equal(t, database.ErrIndexNotFound, err)
	})

	t.Run("Should reindex the right index", func(t *testing.T) {
		tx, _, cleanup := newTestTableFn(t)
		defer cleanup()

		err := tx.ReIndex("a")
		require.NoError(t, err)

		idx, err := tx.GetIndex("a")
		require.NoError(t, err)

		var i int
		err = idx.AscendGreaterOrEqual(index.EmptyPivot(value.Int), func(val value.Value, key []byte) error {
			require.Equal(t, value.NewFloat64(float64(i)), val)
			i++
			return nil
		})
		require.Equal(t, 10, i)
		require.NoError(t, err)

		idx, err = tx.GetIndex("b")
		require.NoError(t, err)

		i = 0
		err = idx.AscendGreaterOrEqual(index.EmptyPivot(value.Int), func(val value.Value, key []byte) error {
			i++
			return nil
		})
		require.NoError(t, err)
		require.Zero(t, i)
	})
}

func TestReIndexAll(t *testing.T) {
	t.Run("Should succeed if not indexes", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.ReIndexAll()
		require.NoError(t, err)
	})

	t.Run("Should reindex all indexes", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateTable("test1", nil)
		require.NoError(t, err)
		tb1, err := tx.GetTable("test1")
		require.NoError(t, err)

		err = tx.CreateTable("test2", nil)
		require.NoError(t, err)
		tb2, err := tx.GetTable("test2")
		require.NoError(t, err)

		for i := 0; i < 10; i++ {
			_, err = tb1.Insert(document.NewFieldBuffer(
				document.NewIntField("a", i),
				document.NewIntField("b", i*10),
			))
			require.NoError(t, err)
			_, err = tb2.Insert(document.NewFieldBuffer(
				document.NewIntField("a", i),
				document.NewIntField("b", i*10),
			))
			require.NoError(t, err)
		}

		err = tx.CreateIndex(index.Options{
			IndexName: "t1a",
			TableName: "test1",
			FieldName: "a",
		})
		require.NoError(t, err)
		err = tx.CreateIndex(index.Options{
			IndexName: "t2a",
			TableName: "test2",
			FieldName: "a",
		})
		require.NoError(t, err)

		err = tx.ReIndexAll()
		require.NoError(t, err)

		idx, err := tx.GetIndex("t1a")
		require.NoError(t, err)

		var i int
		err = idx.AscendGreaterOrEqual(index.EmptyPivot(value.Int), func(val value.Value, key []byte) error {
			require.Equal(t, value.NewFloat64(float64(i)), val)
			i++
			return nil
		})
		require.Equal(t, 10, i)
		require.NoError(t, err)

		idx, err = tx.GetIndex("t2a")
		require.NoError(t, err)

		i = 0
		err = idx.AscendGreaterOrEqual(index.EmptyPivot(value.Int), func(val value.Value, key []byte) error {
			require.Equal(t, value.NewFloat64(float64(i)), val)
			i++
			return nil
		})
		require.Equal(t, 10, i)
		require.NoError(t, err)
	})
}

func newRecord() document.FieldBuffer {
	return document.FieldBuffer([]document.Field{
		document.NewStringField("fielda", "a"),
		document.NewStringField("fieldb", "b"),
	})
}

// TestTableIterate verifies Iterate behaviour.
func TestTableIterate(t *testing.T) {
	t.Run("Should not fail with no records", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		i := 0
		err := tb.Iterate(func(r document.Document) error {
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
		err := tb.Iterate(func(r document.Document) error {
			m[string(r.(document.Keyer).Key())]++
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
		err := tb.Iterate(func(_ document.Document) error {
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
		rec1.Add(document.NewInt64Field("fieldc", 40))
		rec2 := newRecord()

		key1, err := tb.Insert(rec1)
		require.NoError(t, err)
		_, err = tb.Insert(rec2)
		require.NoError(t, err)

		// fetch rec1 and make sure it returns the right one
		res, err := tb.GetRecord(key1)
		require.NoError(t, err)
		fc, err := res.GetValueByName("fieldc")
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

	t.Run("Should use the right field if key is specified", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateTable("test", &database.TableConfig{
			PrimaryKeyName: "foo",
			PrimaryKeyType: value.Int32,
		})
		require.NoError(t, err)
		tb, err := tx.GetTable("test")
		require.NoError(t, err)

		rec := document.NewFieldBuffer(
			document.NewIntField("foo", 1),
			document.NewStringField("bar", "baz"),
		)

		// insert
		key, err := tb.Insert(rec)
		require.NoError(t, err)
		require.Equal(t, value.EncodeInt32(1), key)

		// make sure the record is fetchable using the returned key
		_, err = tb.GetRecord(key)
		require.NoError(t, err)

		// insert again
		key, err = tb.Insert(rec)
		require.Equal(t, database.ErrDuplicateRecord, err)
	})

	t.Run("Should fail if Pk not found in record or empty", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateTable("test", &database.TableConfig{
			PrimaryKeyName: "foo",
			PrimaryKeyType: value.Int,
		})
		require.NoError(t, err)
		tb, err := tx.GetTable("test")
		require.NoError(t, err)

		tests := [][]byte{
			nil,
			[]byte{},
			[]byte(nil),
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("%#v", test), func(t *testing.T) {
				rec := document.NewFieldBuffer(
					document.NewBytesField("foo", test),
				)

				_, err := tb.Insert(rec)
				require.Error(t, err)
			})
		}
	})

	t.Run("Should update indexes if there are indexed fields", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateTable("test", nil)
		require.NoError(t, err)

		err = tx.CreateIndex(index.Options{
			IndexName: "idxFoo", TableName: "test", FieldName: "foo",
		})
		require.NoError(t, err)
		idx, err := tx.GetIndex("idxFoo")
		require.NoError(t, err)

		tb, err := tx.GetTable("test")
		require.NoError(t, err)

		// create one record with the foo field
		rec1 := newRecord()
		foo := document.NewFloat64Field("foo", 10)
		rec1 = append(rec1, foo)

		// create one record without the foo field
		rec2 := newRecord()

		key1, err := tb.Insert(rec1)
		require.NoError(t, err)
		key2, err := tb.Insert(rec2)
		require.NoError(t, err)

		var count int
		err = idx.AscendGreaterOrEqual(nil, func(val value.Value, k []byte) error {
			switch count {
			case 0:
				// key2, which doesn't countain the field must appear first in the next,
				// as null values are the smallest possible values
				require.Equal(t, key2, k)
			case 1:
				require.Equal(t, key1, k)
			}
			count++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, count)
	})
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
		rec1.Add(document.NewInt64Field("fieldc", 40))
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
		require.Equal(t, database.ErrRecordNotFound, err)

		// make sure it didn't also delete the other one
		res, err := tb.GetRecord(key2)
		require.NoError(t, err)
		_, err = res.GetValueByName("fieldc")
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
		rec2 := document.FieldBuffer([]document.Field{
			document.NewStringField("fielda", "c"),
			document.NewStringField("fieldb", "d"),
		})

		key1, err := tb.Insert(rec1)
		require.NoError(t, err)
		key2, err := tb.Insert(rec2)
		require.NoError(t, err)

		// create a third record
		rec3 := document.FieldBuffer([]document.Field{
			document.NewStringField("fielda", "e"),
			document.NewStringField("fieldb", "f"),
		})

		// replace rec1 with rec3
		err = tb.Replace(key1, rec3)
		require.NoError(t, err)

		// make sure it replaced it correctly
		res, err := tb.GetRecord(key1)
		require.NoError(t, err)
		f, err := res.GetValueByName("fielda")
		require.NoError(t, err)
		require.Equal(t, "e", string(f.Data))

		// make sure it didn't also replace the other one
		res, err = tb.GetRecord(key2)
		require.NoError(t, err)
		f, err = res.GetValueByName("fielda")
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

		err = tb.Iterate(func(_ document.Document) error {
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

		err := tx.CreateTable("test1", nil)
		require.NoError(t, err)
		tb, err := tx.GetTable("test1")
		require.NoError(t, err)

		err = tx.CreateTable("test2", nil)
		require.NoError(t, err)

		err = tx.CreateIndex(index.Options{
			Unique:    true,
			IndexName: "idx1a",
			TableName: "test1",
			FieldName: "a",
		})
		require.NoError(t, err)
		err = tx.CreateIndex(index.Options{
			Unique:    false,
			IndexName: "idx1b",
			TableName: "test1",
			FieldName: "b",
		})
		require.NoError(t, err)
		err = tx.CreateIndex(index.Options{
			Unique:    false,
			IndexName: "ifx2a",
			TableName: "test2",
			FieldName: "a",
		})
		require.NoError(t, err)

		m, err := tb.Indexes()
		require.NoError(t, err)
		require.Len(t, m, 2)
		idx1a, ok := m["a"]
		require.True(t, ok)
		require.NotNil(t, idx1a)
		idx1b, ok := m["b"]
		require.True(t, ok)
		require.NotNil(t, idx1b)
	})
}

// BenchmarkTableInsert benchmarks the Insert method with 1, 10, 1000 and 10000 successive insertions.
func BenchmarkTableInsert(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			var fields []document.Field

			for i := int64(0); i < 10; i++ {
				fields = append(fields, document.NewInt64Field(fmt.Sprintf("name-%d", i), i))
			}

			rec := document.FieldBuffer(fields)

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

			var fields []document.Field

			for i := int64(0); i < 10; i++ {
				fields = append(fields, document.NewInt64Field(fmt.Sprintf("name-%d", i), i))
			}

			rec := document.FieldBuffer(fields)

			for i := 0; i < size; i++ {
				_, err := tb.Insert(rec)
				require.NoError(b, err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tb.Iterate(func(document.Document) error {
					return nil
				})
			}
			b.StopTimer()
		})
	}
}

func TestTxListTables(t *testing.T) {
	t.Run("Should succeed if not tables", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		list, err := tx.ListTables()
		require.NoError(t, err)
		require.Len(t, list, 0)
	})

	t.Run("Should return the right tables", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.CreateTable("a", nil)
		require.NoError(t, err)
		err = tx.CreateTable("b", nil)
		require.NoError(t, err)

		err = tx.CreateIndex(index.Options{
			IndexName: "idxa",
			TableName: "a",
			FieldName: "foo",
		})
		require.NoError(t, err)

		list, err := tx.ListTables()
		require.NoError(t, err)
		require.Len(t, list, 2)
		require.Equal(t, []string{"a", "b"}, list)
	})
}
