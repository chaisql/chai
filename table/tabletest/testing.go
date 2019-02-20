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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.test(t, builder)
		})
	}
}

func newRecord() record.Record {
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
		err := tb.Iterate(func(record.Record) bool {
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

		// TODO(asdine) change the iterate signature to pass the rowid
		// and use a map to ensure all the rowids have been returned (not necessarily in order)
		i := 0
		err := tb.Iterate(func(record.Record) bool {
			i++
			return true
		})
		require.NoError(t, err)
		require.Equal(t, 10, i)
	})
}
