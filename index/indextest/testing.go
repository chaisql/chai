package indextest

import (
	"testing"

	"github.com/asdine/genji/index"
	"github.com/stretchr/testify/require"
)

// Builder is a function that can create an index on demand and that provides
// a function to cleanup up and remove any created state.
// Tests will use the builder like this:
//     idx, cleanup := builder()
//     defer cleanup()
//     ...
type Builder func() (index.Index, func())

// TestSuite tests all of the methods of an index.
func TestSuite(t *testing.T, builder Builder) {
	tests := []struct {
		name string
		test func(*testing.T, Builder)
	}{
		{"Index/Cursor", TestIndexCursor},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.test(t, builder)
		})
	}
}

// TestIndexCursor verifies Cursor behaviour.
func TestIndexCursor(t *testing.T, builder Builder) {
	t.Run("Can create multiple cursor", func(t *testing.T) {
		idx, cleanup := builder()
		defer cleanup()

		c1 := idx.Cursor()
		require.NotNil(t, c1)
		c2 := idx.Cursor()
		require.NotNil(t, c2)
	})
}
