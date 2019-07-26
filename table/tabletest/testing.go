// Package tabletest defines a list of tests that can be used to test
// table implementations.
package tabletest

import (
	"errors"
	"fmt"
	"testing"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	"github.com/stretchr/testify/require"
)

// Builder is a function that can create a table on demand and that provides
// a function to cleanup up and remove any created state.
// Tests will use the builder like this:
//     tb, cleanup := builder()
//     defer cleanup()
//     ...
type Builder func() (table.Table, func())

// TestSuite tests all of the methods of a table.
func TestSuite(t *testing.T, builder Builder) {
	tests := []struct {
		name string
		test func(*testing.T, Builder)
	}{
		{"TableReader/Iterate", TestTableReaderIterate},
		{"TableReader/Record", TestTableReaderRecord},
		{"TableWriter/Insert", TestTableWriterInsert},
		{"TableWriter/Delete", TestTableWriterDelete},
		{"TableWriter/Replace", TestTableWriterReplace},
		{"TableWriter/Truncate", TestTableWriterTruncate},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.test(t, builder)
		})
	}
}

func newRecord() record.FieldBuffer {
	return record.FieldBuffer([]field.Field{
		field.NewString("fielda", "a"),
		field.NewString("fieldb", "b"),
	})
}

// TestTableReaderIterate verifies Iterate behaviour.
func TestTableReaderIterate(t *testing.T, builder Builder) {
	t.Run("Should not fail with no records", func(t *testing.T) {
		tb, cleanup := builder()
		defer cleanup()

		i := 0
		err := tb.Iterate(func(recordID []byte, r record.Record) error {
			i++
			return nil
		})
		require.NoError(t, err)
		require.Zero(t, i)
	})

	t.Run("Should iterate over all records", func(t *testing.T) {
		tb, cleanup := builder()
		defer cleanup()

		for i := 0; i < 10; i++ {
			_, err := tb.Insert(newRecord())
			require.NoError(t, err)
		}

		m := make(map[string]int)
		err := tb.Iterate(func(recordID []byte, _ record.Record) error {
			m[string(recordID)]++
			return nil
		})
		require.NoError(t, err)
		require.Len(t, m, 10)
		for _, c := range m {
			require.Equal(t, 1, c)
		}
	})

	t.Run("Should stop if fn returns error", func(t *testing.T) {
		tb, cleanup := builder()
		defer cleanup()

		for i := 0; i < 10; i++ {
			_, err := tb.Insert(newRecord())
			require.NoError(t, err)
		}

		i := 0
		err := tb.Iterate(func(recordID []byte, _ record.Record) error {
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

// TestTableReaderRecord verifies Record behaviour.
func TestTableReaderRecord(t *testing.T, builder Builder) {
	t.Run("Should fail if not found", func(t *testing.T) {
		tb, cleanup := builder()
		defer cleanup()

		r, err := tb.Record([]byte("id"))
		require.Equal(t, table.ErrRecordNotFound, err)
		require.Nil(t, r)
	})

	t.Run("Should return the right record", func(t *testing.T) {
		tb, cleanup := builder()
		defer cleanup()

		// create two records, one with an additional field
		rec1 := newRecord()
		rec1.Add(field.NewInt64("fieldc", 40))
		rec2 := newRecord()

		recordID1, err := tb.Insert(rec1)
		require.NoError(t, err)
		_, err = tb.Insert(rec2)
		require.NoError(t, err)

		// fetch rec1 and make sure it returns the right one
		res, err := tb.Record(recordID1)
		require.NoError(t, err)
		fc, err := res.Field("fieldc")
		require.NoError(t, err)
		require.Equal(t, rec1[2], fc)
	})
}

// TestTableWriterInsert verifies Insert behaviour.
func TestTableWriterInsert(t *testing.T, builder Builder) {
	t.Run("Should generate a recordID by default", func(t *testing.T) {
		tb, cleanup := builder()
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

	t.Run("Should support Pker interface", func(t *testing.T) {
		tb, cleanup := builder()
		defer cleanup()

		var counter int64

		rec := recordPker{
			pkGenerator: func() ([]byte, error) {
				counter += 2
				return field.EncodeInt64(counter), nil
			},
		}

		// insert
		recordID, err := tb.Insert(rec)
		require.NoError(t, err)
		require.Equal(t, field.EncodeInt64(2), recordID)

		// make sure the record is fetchable using the returned recordID
		_, err = tb.Record(recordID)
		require.NoError(t, err)

		// insert again
		recordID, err = tb.Insert(rec)
		require.NoError(t, err)
		require.Equal(t, field.EncodeInt64(4), recordID)
	})

	t.Run("Should fail if Pk returns empty recordID", func(t *testing.T) {
		tb, cleanup := builder()
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
						return nil, nil
					},
				}

				_, err := tb.Insert(rec)
				require.Error(t, err)
			})
		}
	})
}

type recordPker struct {
	record.FieldBuffer
	pkGenerator func() ([]byte, error)
}

func (r recordPker) Pk() ([]byte, error) {
	return r.pkGenerator()
}

// TestTableWriterDelete verifies Delete behaviour.
func TestTableWriterDelete(t *testing.T, builder Builder) {
	t.Run("Should fail if not found", func(t *testing.T) {
		tb, cleanup := builder()
		defer cleanup()

		err := tb.Delete([]byte("id"))
		require.Equal(t, table.ErrRecordNotFound, err)
	})

	t.Run("Should delete the right record", func(t *testing.T) {
		tb, cleanup := builder()
		defer cleanup()

		// create two records, one with an additional field
		rec1 := newRecord()
		rec1.Add(field.NewInt64("fieldc", 40))
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
		require.Equal(t, table.ErrRecordNotFound, err)

		// make sure it didn't also delete the other one
		res, err := tb.Record(recordID2)
		require.NoError(t, err)
		_, err = res.Field("fieldc")
		require.Error(t, err)
	})
}

// TestTableWriterReplace verifies Replace behaviour.
func TestTableWriterReplace(t *testing.T, builder Builder) {
	t.Run("Should fail if not found", func(t *testing.T) {
		tb, cleanup := builder()
		defer cleanup()

		err := tb.Replace([]byte("id"), newRecord())
		require.Equal(t, table.ErrRecordNotFound, err)
	})

	t.Run("Should replace the right record", func(t *testing.T) {
		tb, cleanup := builder()
		defer cleanup()

		// create two different records
		rec1 := newRecord()
		rec2 := record.FieldBuffer([]field.Field{
			field.NewString("fielda", "c"),
			field.NewString("fieldb", "d"),
		})

		recordID1, err := tb.Insert(rec1)
		require.NoError(t, err)
		recordID2, err := tb.Insert(rec2)
		require.NoError(t, err)

		// create a third record
		rec3 := record.FieldBuffer([]field.Field{
			field.NewString("fielda", "e"),
			field.NewString("fieldb", "f"),
		})

		// replace rec1 with rec3
		err = tb.Replace(recordID1, rec3)
		require.NoError(t, err)

		// make sure it replaced it correctly
		res, err := tb.Record(recordID1)
		require.NoError(t, err)
		f, err := res.Field("fielda")
		require.NoError(t, err)
		require.Equal(t, "e", string(f.Data))

		// make sure it didn't also replace the other one
		res, err = tb.Record(recordID2)
		require.NoError(t, err)
		f, err = res.Field("fielda")
		require.NoError(t, err)
		require.Equal(t, "c", string(f.Data))
	})
}

// TestTableWriterTruncate verifies Truncate behaviour.
func TestTableWriterTruncate(t *testing.T, builder Builder) {
	t.Run("Should succeed if table empty", func(t *testing.T) {
		tb, cleanup := builder()
		defer cleanup()

		err := tb.Truncate()
		require.NoError(t, err)
	})

	t.Run("Should truncate the table", func(t *testing.T) {
		tb, cleanup := builder()
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

		err = tb.Iterate(func(_ []byte, _ record.Record) error {
			return errors.New("should not iterate")
		})

		require.NoError(t, err)
	})
}
