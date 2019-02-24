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
		err := tb.Iterate(func(rowid []byte, r record.Record) error {
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
		err := tb.Iterate(func(rowid []byte, _ record.Record) error {
			m[string(rowid)]++
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
		err := tb.Iterate(func(rowid []byte, _ record.Record) error {
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

		rowid1, err := tb.Insert(rec1)
		require.NoError(t, err)
		_, err = tb.Insert(rec2)
		require.NoError(t, err)

		// fetch rec1 and make sure it returns the right one
		res, err := tb.Record(rowid1)
		require.NoError(t, err)
		fc, err := res.Field("fieldc")
		require.NoError(t, err)
		require.Equal(t, rec1[2], fc)
	})
}

// TestTableWriterInsert verifies Insert behaviour.
func TestTableWriterInsert(t *testing.T, builder Builder) {
	t.Run("Should generate a rowid by default", func(t *testing.T) {
		tb, cleanup := builder()
		defer cleanup()

		rec := newRecord()
		rowid1, err := tb.Insert(rec)
		require.NoError(t, err)
		require.NotEmpty(t, rowid1)

		rowid2, err := tb.Insert(rec)
		require.NoError(t, err)
		require.NotEmpty(t, rowid2)

		require.NotEqual(t, rowid1, rowid2)
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
		rowid, err := tb.Insert(rec)
		require.NoError(t, err)
		require.Equal(t, field.EncodeInt64(2), rowid)

		// make sure the record is fetchable using the returned rowid
		_, err = tb.Record(rowid)
		require.NoError(t, err)

		// insert again
		rowid, err = tb.Insert(rec)
		require.NoError(t, err)
		require.Equal(t, field.EncodeInt64(4), rowid)
	})

	t.Run("Should fail if Pk returns empty rowid", func(t *testing.T) {
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

		rowid1, err := tb.Insert(rec1)
		require.NoError(t, err)
		rowid2, err := tb.Insert(rec2)
		require.NoError(t, err)

		// delete the record
		err = tb.Delete([]byte(rowid1))
		require.NoError(t, err)

		// try again, should fail
		err = tb.Delete([]byte(rowid1))
		require.Equal(t, table.ErrRecordNotFound, err)

		// make sure it didn't also delete the other one
		res, err := tb.Record(rowid2)
		require.NoError(t, err)
		_, err = res.Field("fieldc")
		require.Error(t, err)
	})
}
