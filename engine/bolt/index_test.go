package bolt

import (
	"path"
	"testing"

	"github.com/asdine/genji/field"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/index/indextest"
	"github.com/stretchr/testify/require"
)

func TestBoltEngineIndex(t *testing.T) {
	indextest.TestSuite(t, func() (index.Index, func()) {
		dir, cleanup := tempDir(t)
		ng, err := NewEngine(path.Join(dir, "test.db"), 0600, nil)
		require.NoError(t, err)

		tx, err := ng.Begin(true)
		require.NoError(t, err)

		_, err = tx.CreateTable("test")
		require.NoError(t, err)

		idx, err := tx.CreateIndex("test", "idx")
		require.NoError(t, err)

		return idx, func() {
			tx.Rollback()
			ng.Close()
			cleanup()
		}
	})
}

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

	require.Equal(t, 3, countItems(t, b))
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
	val, rowid := c.First()
	require.Equal(t, d2, val)
	require.Equal(t, field.EncodeInt64(0), rowid)

	for i := 1; i < 10; i++ {
		val, rowid := c.Next()
		require.Equal(t, d2, val)
		require.Equal(t, field.EncodeInt64(int64(i)), rowid)
	}

	val, rowid = c.Next()
	require.Equal(t, d1, val)
	require.Equal(t, field.EncodeInt64(20), rowid)

	val, rowid = c.Next()
	require.Nil(t, val)
	require.Nil(t, rowid)

	for i := 9; i >= 0; i-- {
		val, rowid := c.Prev()
		require.Equal(t, d2, val)
		require.Equal(t, field.EncodeInt64(int64(i)), rowid)
	}

	val, rowid = c.Prev()
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
	val, rowid := c.First()
	require.Equal(t, d1, val)
	require.Equal(t, field.EncodeInt64(0), rowid)

	val, rowid = c.Last()
	require.Equal(t, d2, val)
	require.Equal(t, field.EncodeInt64(5), rowid)

	val, rowid = c.Seek(d1)
	require.Equal(t, d1, val)
	require.Equal(t, field.EncodeInt64(0), rowid)

	val, rowid = c.Seek(d2)
	require.Equal(t, d2, val)
	require.Equal(t, field.EncodeInt64(3), rowid)

	val, rowid = c.Seek([]byte("jac"))
	require.Equal(t, d1, val)
	require.Equal(t, field.EncodeInt64(0), rowid)

	val, rowid = c.Seek([]byte("jackk"))
	require.Equal(t, d2, val)
	require.Equal(t, field.EncodeInt64(3), rowid)

	val, rowid = c.Prev()
	require.Equal(t, d1, val)
	require.Equal(t, field.EncodeInt64(2), rowid)
}

func TestIndexSeek(t *testing.T) {
	b, cleanup := tempBucket(t, true)
	defer cleanup()

	idx := Index{b: b}

	d1 := []byte("jack")
	d2 := []byte("john")

	err := idx.Set(d1, field.EncodeInt64(int64(10)))
	require.NoError(t, err)

	err = idx.Set(d2, field.EncodeInt64(int64(20)))
	require.NoError(t, err)

	c := idx.Cursor()
	val, rowid := c.Seek([]byte("jack"))
	require.Equal(t, d1, val)
	require.Equal(t, field.EncodeInt64(10), rowid)
	val, rowid = c.Next()
	require.Equal(t, d2, val)
	require.Equal(t, field.EncodeInt64(20), rowid)

	val, rowid = c.Prev()
	require.Equal(t, d1, val)
	require.Equal(t, field.EncodeInt64(10), rowid)
	val, rowid = c.Prev()
	require.Nil(t, val)
	require.Nil(t, rowid)
	val, rowid = c.Next()
	require.Equal(t, d2, val)
	require.Equal(t, field.EncodeInt64(20), rowid)

	val, rowid = c.Seek([]byte("john"))
	require.Equal(t, d2, val)
	require.Equal(t, field.EncodeInt64(20), rowid)
}
