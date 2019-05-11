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
		{"Index/Set", TestIndexSet},
		{"Index/Delete", TestIndexDelete},
		{"Index/Cursor", TestIndexCursor},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.test(t, builder)
		})
	}
}

// TestIndexSet verifies Set behaviour.
func TestIndexSet(t *testing.T, builder Builder) {
	idx, cleanup := builder()
	defer cleanup()

	t.Run("Set nil value fails", func(t *testing.T) {
		require.Error(t, idx.Set(nil, []byte("rid")))
		require.Error(t, idx.Set([]byte{}, []byte("rid")))
	})

	t.Run("Set nil rowid succeeds", func(t *testing.T) {
		require.NoError(t, idx.Set([]byte("value"), nil))
	})

	t.Run("Set value and rowid succeeds", func(t *testing.T) {
		require.NoError(t, idx.Set([]byte("value"), []byte("rowid")))
	})
}

// TestIndexDelete verifies Delete behaviour.
func TestIndexDelete(t *testing.T, builder Builder) {
	idx, cleanup := builder()
	defer cleanup()

	t.Run("Delete valid rowid succeeds", func(t *testing.T) {
		require.NoError(t, idx.Set([]byte("value"), []byte("rowid")))
		require.NoError(t, idx.Delete([]byte("rowid")))
		require.Error(t, idx.Delete([]byte("rowid")))
	})

	t.Run("Delete nil rowid fails", func(t *testing.T) {
		require.Error(t, idx.Delete(nil))
	})

	t.Run("Delete non existing rowid fails", func(t *testing.T) {
		require.Error(t, idx.Delete([]byte("foo")))
	})
}

// TestIndexCursor verifies Cursor behaviour.
func TestIndexCursor(t *testing.T, builder Builder) {
	t.Run("Can create multiple cursors", func(t *testing.T) {
		idx, cleanup := builder()
		defer cleanup()

		c1 := idx.Cursor()
		require.NotNil(t, c1)
		c2 := idx.Cursor()
		require.NotNil(t, c2)
	})

	t.Run("Returns nil if index is empty", func(t *testing.T) {
		idx, cleanup := builder()
		defer cleanup()

		c := idx.Cursor()
		tests := []func() ([]byte, []byte){
			c.First,
			c.Last,
			c.Next,
			c.Prev,
			func() ([]byte, []byte) { return c.Seek(nil) },
		}

		for _, fn := range tests {
			v, rid := fn()
			require.Nil(t, v)
			require.Nil(t, rid)
		}
	})

	t.Run("First returns the lowest value", func(t *testing.T) {
		idx, cleanup := builder()
		defer cleanup()

		require.NoError(t, idx.Set([]byte("B"), []byte("A")))
		require.NoError(t, idx.Set([]byte("A"), []byte("B")))

		v, rid := idx.Cursor().First()
		require.Equal(t, []byte("A"), v)
		require.Equal(t, []byte("B"), rid)
	})

	t.Run("Last returns the biggest value", func(t *testing.T) {
		idx, cleanup := builder()
		defer cleanup()

		require.NoError(t, idx.Set([]byte("B"), []byte("A")))
		require.NoError(t, idx.Set([]byte("A"), []byte("B")))

		v, rid := idx.Cursor().Last()
		require.Equal(t, []byte("B"), v)
		require.Equal(t, []byte("A"), rid)
	})

	idx, cleanup := builder()
	defer cleanup()

	for i := byte(0); i < 10; i += 2 {
		require.NoError(t, idx.Set([]byte{'A' + i}, []byte{'a' + i}))
	}

	t.Run("Next after Last returns nil", func(t *testing.T) {
		c := idx.Cursor()

		c.Last()
		v, rid := c.Next()
		require.Nil(t, v)
		require.Nil(t, rid)
	})

	t.Run("Prev after First returns nil", func(t *testing.T) {
		c := idx.Cursor()

		c.First()
		v, rid := c.Prev()
		require.Nil(t, v)
		require.Nil(t, rid)
	})

	t.Run("Ascending iteration", func(t *testing.T) {
		c := idx.Cursor()

		var i byte

		for v, rid := c.First(); v != nil; v, rid = c.Next() {
			require.Equal(t, []byte{'A' + i}, v)
			require.Equal(t, []byte{'a' + i}, rid)
			i += 2
		}
	})

	t.Run("Descending iteration", func(t *testing.T) {
		c := idx.Cursor()

		var i byte = 8

		for v, rid := c.Last(); v != nil; v, rid = c.Prev() {
			require.Equal(t, []byte{'A' + i}, v)
			require.Equal(t, []byte{'a' + i}, rid)
			i -= 2
		}
	})

	t.Run("Ascending and descending iteration", func(t *testing.T) {
		c := idx.Cursor()

		var i byte

		for v, rid := c.First(); v != nil; v, rid = c.Next() {
			require.Equal(t, []byte{'A' + i}, v)
			require.Equal(t, []byte{'a' + i}, rid)
			i += 2
		}

		i -= 2
		for v, rid := c.Last(); v != nil; v, rid = c.Prev() {
			require.Equal(t, []byte{'A' + i}, v)
			require.Equal(t, []byte{'a' + i}, rid)
			i -= 2
		}
	})

	t.Run("Next Prev Next", func(t *testing.T) {
		c := idx.Cursor()

		c.First()
		v1, rid1 := c.Next()
		c.Prev()
		v2, rid2 := c.Next()
		require.Equal(t, v1, v2)
		require.Equal(t, rid1, rid2)
	})

	t.Run("Prev Next Prev", func(t *testing.T) {
		c := idx.Cursor()

		c.Last()
		v1, rid1 := c.Prev()
		c.Next()
		v2, rid2 := c.Prev()
		require.Equal(t, v1, v2)
		require.Equal(t, rid1, rid2)
	})

	t.Run("Seek finds the right value", func(t *testing.T) {
		c := idx.Cursor()

		v, rid := c.Seek([]byte{'C'})
		require.Equal(t, []byte{'C'}, v)
		require.Equal(t, []byte{'c'}, rid)
	})

	t.Run("Seek returns next value if not found", func(t *testing.T) {
		c := idx.Cursor()

		v, rid := c.Seek([]byte{'D'})
		require.Equal(t, []byte{'E'}, v)
		require.Equal(t, []byte{'e'}, rid)

		v, rid = c.Seek([]byte{'M'})
		require.Nil(t, v)
		require.Nil(t, rid)
	})

	t.Run("Seek then Next", func(t *testing.T) {
		c := idx.Cursor()

		c.Seek([]byte{'B'})
		v, rid := c.Next()
		require.Equal(t, []byte{'E'}, v)
		require.Equal(t, []byte{'e'}, rid)
	})

	t.Run("Seek then Prev", func(t *testing.T) {
		c := idx.Cursor()

		c.Seek([]byte{'B'})
		v, rid := c.Prev()
		require.Equal(t, []byte{'A'}, v)
		require.Equal(t, []byte{'a'}, rid)
	})
}
