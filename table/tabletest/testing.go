// Package tabletest defines a list of tests that can be used to test
// table implementations.
package tabletest

import (
	"testing"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	"github.com/stretchr/testify/require"
)

// Builder is a function that can create a table on demand and that provides
// a function to cleanup up and remove any created state.
// Tests will use the builder like this:
//     ng, cleanup := builder()
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
		err := tb.Iterate(func(rowid []byte, r record.Record) bool {
			i++
			return true
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
		err := tb.Iterate(func(rowid []byte, _ record.Record) bool {
			m[string(rowid)]++
			return true
		})
		require.NoError(t, err)
		require.Len(t, m, 10)
		for _, c := range m {
			require.Equal(t, 1, c)
		}
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
