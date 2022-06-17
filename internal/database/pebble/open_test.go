package pebble

import (
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/genjidb/genji/internal/encoding"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func encodeKey(t testing.TB, values ...types.Value) []byte {
	t.Helper()

	k := tree.NewKey(values...)

	b, err := k.Encode(0)
	require.NoError(t, err)
	return b
}

func TestOpen(t *testing.T) {
	var opts pebble.Options
	opts.FS = vfs.NewMem()

	db, err := Open("", &opts)
	require.NoError(t, err)
	defer db.Close()

	k1 := encodeKey(t, types.NewIntegerValue(1), types.NewIntegerValue(1))
	k2 := encodeKey(t, types.NewIntegerValue(1), types.NewIntegerValue(2))

	// set keys normally
	err = db.Set(k1, []byte("1"), nil)
	require.NoError(t, err)
	err = db.Set(k2, []byte("2"), nil)
	require.NoError(t, err)

	// set keys in batch
	k3 := encodeKey(t, types.NewIntegerValue(1), types.NewIntegerValue(3))
	k4 := encodeKey(t, types.NewIntegerValue(1), types.NewIntegerValue(4))
	b := db.NewIndexedBatch()
	err = b.Set(k3, []byte("3"), nil)
	require.NoError(t, err)
	err = b.Set(k4, []byte("4"), nil)
	require.NoError(t, err)

	iterate := func() {
		it := b.NewIter(&pebble.IterOptions{
			LowerBound: encodeKey(t, types.NewIntegerValue(1)),
		})
		it.First()
		require.Equal(t, k1, it.Key())
		it.Next()
		require.Equal(t, k2, it.Key())
		it.Next()
		require.Equal(t, k3, it.Key())
		it.Next()
		require.Equal(t, k4, it.Key())
		it.Next()
		require.False(t, it.Valid())
		it.Close()
	}

	// iterate on the batch
	iterate()

	// commit
	err = b.Commit(nil)
	require.NoError(t, err)

	// iterate on the db
	iterate()
}

func TestPebbleBatchSet(t *testing.T) {
	var opts pebble.Options
	opts.FS = vfs.NewMem()

	opts.Comparer = WithStats(DefaultComparer)

	db, err := Open("", &opts)
	require.NoError(t, err)
	defer db.Close()

	v := []byte("1")

	keys := make([][]byte, 1000)
	for i := int64(0); i < 1000; i++ {
		keys[i] = encodeKey(t, types.NewIntegerValue(i), types.NewIntegerValue(i))
	}

	for i := 0; i < 1000; i++ {
		batch := db.NewIndexedBatch()
		for j := int64(0); j < 1000; j++ {
			// set keys normally
			err = batch.Set(keys[j], v, nil)
			require.NoError(t, err)
		}
		err = batch.Commit(nil)
		require.NoError(t, err)
	}
}

func BenchmarkSetWithCustomComparer(b *testing.B) {
	var opts pebble.Options
	opts.FS = vfs.NewMem()

	db, err := Open("", &opts)
	require.NoError(b, err)
	defer db.Close()

	v := []byte("1")

	keys := make([][]byte, 1000)
	for i := int64(0); i < 1000; i++ {
		keys[i] = encodeKey(b, types.NewIntegerValue(i), types.NewIntegerValue(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := int64(0); j < 1000; j++ {
			// set keys normally
			db.Set(keys[0], v, nil)
		}
	}
	b.StopTimer()
}

func BenchmarkBatchSetWithCustomComparer(b *testing.B) {
	var opts pebble.Options
	opts.FS = vfs.NewMem()

	db, err := Open("", &opts)
	require.NoError(b, err)
	defer db.Close()

	v := []byte("1")

	keys := make([][]byte, 1000)
	for i := int64(0); i < 1000; i++ {
		keys[i] = encodeKey(b, types.NewIntegerValue(i), types.NewIntegerValue(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := db.NewIndexedBatch()
		for j := int64(0); j < 1000; j++ {
			// set keys normally
			batch.Set(keys[j], v, nil)
		}
		batch.Commit(nil)
	}
	b.StopTimer()
}

func BenchmarkSetWithDefaultComparer(b *testing.B) {
	var opts pebble.Options
	opts.FS = vfs.NewMem()

	db, err := pebble.Open("", &opts)
	require.NoError(b, err)
	defer db.Close()

	v := []byte("1")

	keys := make([][]byte, 1000)
	for i := int64(0); i < 1000; i++ {
		keys[i] = encodeKey(b, types.NewIntegerValue(i), types.NewIntegerValue(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := int64(0); j < 1000; j++ {
			// set keys normally
			db.Set(keys[j], v, nil)
		}
	}
	b.StopTimer()
}

func BenchmarkBatchSetWithDefaultComparer(b *testing.B) {
	var opts pebble.Options
	opts.FS = vfs.NewMem()

	db, err := pebble.Open("", &opts)
	require.NoError(b, err)
	defer db.Close()

	v := []byte("1")

	keys := make([][]byte, 1000)
	for i := int64(0); i < 1000; i++ {
		keys[i] = encodeKey(b, types.NewIntegerValue(i), types.NewIntegerValue(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := db.NewIndexedBatch()
		for j := int64(0); j < 1000; j++ {
			// set keys normally
			batch.Set(keys[j], v, nil)
		}

		batch.Commit(nil)
	}
	b.StopTimer()
}

func BenchmarkCustomCompare(b *testing.B) {
	k := encodeKey(b, types.NewIntegerValue(1), types.NewIntegerValue(1))

	for i := 0; i < b.N; i++ {
		encoding.Compare(k, k)
	}
}

func BenchmarkDefaultCompare(b *testing.B) {
	k := encodeKey(b, types.NewIntegerValue(1), types.NewIntegerValue(1))

	for i := 0; i < b.N; i++ {
		pebble.DefaultComparer.Compare(k, k)
	}
}
