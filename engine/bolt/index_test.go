package bolt

import (
	"testing"

	"github.com/asdine/genji/field"
	"github.com/stretchr/testify/require"
)

func TestIndexSet(t *testing.T) {
	b, cleanup := tempBucket(t, true)
	defer cleanup()

	idx := Index{b: b}
	d1 := []byte("john")
	d2 := []byte("jack")

	err := idx.Set(d1, []byte("1"))
	require.NoError(t, err)
	err = idx.Set(d1, []byte("2"))
	require.NoError(t, err)
	err = idx.Set(d2, []byte("3"))
	require.NoError(t, err)

	require.Equal(t, 2, countItems(t, b))

	bb := b.Bucket(d1)
	require.NotNil(t, bb)
	require.Equal(t, 2, countItems(t, bb))

	bb = b.Bucket(d2)
	require.NotNil(t, bb)
	require.Equal(t, 1, countItems(t, bb))
}

func TestIndexNextPrev(t *testing.T) {
	b, cleanup := tempBucket(t, true)
	defer cleanup()

	idx := Index{b: b}
	d1 := []byte("john")
	d2 := []byte("jack")

	err := idx.Set(d1, field.EncodeInt64(20))
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		err := idx.Set(d2, field.EncodeInt64(int64(i)))
		require.NoError(t, err)
	}

	c := idx.Cursor()
	val, rowid, err := c.First()
	require.NoError(t, err)
	require.Equal(t, d2, val)
	require.Equal(t, field.EncodeInt64(0), rowid)

	for i := 1; i < 10; i++ {
		val, rowid, err := c.Next()
		require.NoError(t, err)
		require.Equal(t, d2, val)
		require.Equal(t, field.EncodeInt64(int64(i)), rowid)
	}

	val, rowid, err = c.Next()
	require.NoError(t, err)
	require.Equal(t, d1, val)
	require.Equal(t, field.EncodeInt64(20), rowid)

	val, rowid, err = c.Next()
	require.NoError(t, err)
	require.Nil(t, val)
	require.Nil(t, rowid)

	for i := 9; i >= 0; i-- {
		val, rowid, err := c.Prev()
		require.NoError(t, err)
		require.Equal(t, d2, val)
		require.Equal(t, field.EncodeInt64(int64(i)), rowid)
	}

	val, rowid, err = c.Prev()
	require.NoError(t, err)
	require.Nil(t, val)
	require.Nil(t, rowid)
}

func TestIndexFirstLast(t *testing.T) {
	b, cleanup := tempBucket(t, true)
	defer cleanup()

	idx := Index{b: b}
	d1 := []byte("jack")
	d2 := []byte("john")

	for i := 0; i < 3; i++ {
		err := idx.Set(d1, field.EncodeInt64(int64(i)))
		require.NoError(t, err)
	}

	for i := 3; i < 6; i++ {
		err := idx.Set(d2, field.EncodeInt64(int64(i)))
		require.NoError(t, err)
	}

	c := idx.Cursor()
	val, rowid, err := c.First()
	require.NoError(t, err)
	require.Equal(t, d1, val)
	require.Equal(t, field.EncodeInt64(0), rowid)

	val, rowid, err = c.Last()
	require.NoError(t, err)
	require.Equal(t, d2, val)
	require.Equal(t, field.EncodeInt64(5), rowid)
}
