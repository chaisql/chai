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
	f1 := field.Field{
		Data: []byte("john"),
	}
	f2 := field.Field{
		Data: []byte("jack"),
	}

	err := idx.Set(f1, []byte("1"))
	require.NoError(t, err)
	err = idx.Set(f1, []byte("2"))
	require.NoError(t, err)
	err = idx.Set(f2, []byte("3"))
	require.NoError(t, err)

	require.Equal(t, 2, countItems(t, b))

	bb := b.Bucket(f1.Data)
	require.NotNil(t, bb)
	require.Equal(t, 2, countItems(t, bb))

	bb = b.Bucket(f2.Data)
	require.NotNil(t, bb)
	require.Equal(t, 1, countItems(t, bb))
}
