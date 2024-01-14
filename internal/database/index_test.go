package database_test

import (
	"fmt"
	"testing"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/kv"
	"github.com/chaisql/chai/internal/object"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/testutil/assert"
	"github.com/chaisql/chai/internal/tree"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// values is a helper function to avoid having to type []types.Value{} all the time.
func values(vs ...types.Value) []types.Value {
	return vs
}

func getIndex(t testing.TB, arity int) *database.Index {
	st, err := kv.NewEngine(":memory:", kv.Options{
		RollbackSegmentNamespace: int64(database.RollbackSegmentNamespace),
		MaxBatchSize:             1 << 7,
		MinTransientNamespace:    10_000,
		MaxTransientNamespace:    11_000,
	})
	require.NoError(t, err)

	session := st.NewBatchSession()

	tr := tree.New(session, 10, 0)

	var paths []object.Path
	for i := 0; i < arity; i++ {
		paths = append(paths, object.NewPath(fmt.Sprintf("[%d]", i)))
	}
	idx := database.NewIndex(tr, database.IndexInfo{Paths: paths})

	t.Cleanup(func() {
		session.Close()
	})

	return idx
}

func TestIndexSet(t *testing.T) {
	t.Run("Set nil key falls (arity=1)", func(t *testing.T) {
		idx := getIndex(t, 1)
		assert.Error(t, idx.Set(values(types.NewBooleanValue(true)), nil))
	})

	t.Run("Set value and key succeeds (arity=1)", func(t *testing.T) {
		idx := getIndex(t, 1)
		assert.NoError(t, idx.Set(values(types.NewBooleanValue(true)), []byte("key")))
	})

	t.Run("Set two values and key succeeds (arity=2)", func(t *testing.T) {
		idx := getIndex(t, 2)
		assert.NoError(t, idx.Set(values(types.NewBooleanValue(true), types.NewBooleanValue(true)), []byte("key")))
	})

	t.Run("Set one value fails (arity=1)", func(t *testing.T) {
		idx := getIndex(t, 2)
		assert.Error(t, idx.Set(values(types.NewBooleanValue(true)), []byte("key")))
	})

	t.Run("Set two values fails (arity=1)", func(t *testing.T) {
		idx := getIndex(t, 1)
		assert.Error(t, idx.Set(values(types.NewBooleanValue(true), types.NewBooleanValue(true)), []byte("key")))
	})

	t.Run("Set three values fails (arity=2)", func(t *testing.T) {
		idx := getIndex(t, 2)
		assert.Error(t, idx.Set(values(types.NewBooleanValue(true), types.NewBooleanValue(true), types.NewBooleanValue(true)), []byte("key")))
	})
}

func TestIndexDelete(t *testing.T) {
	t.Run("Delete valid key succeeds", func(t *testing.T) {
		idx := getIndex(t, 1)

		assert.NoError(t, idx.Set(values(types.NewDoubleValue(10)), []byte("key")))
		assert.NoError(t, idx.Set(values(types.NewIntegerValue(10)), []byte("other-key")))
		assert.NoError(t, idx.Set(values(types.NewIntegerValue(11)), []byte("yet-another-key")))
		assert.NoError(t, idx.Set(values(types.NewTextValue("hello")), []byte("yet-another-different-key")))
		assert.NoError(t, idx.Delete(values(types.NewDoubleValue(10)), []byte("key")))

		pivot := values(types.NewIntegerValue(10))
		i := 0
		err := idx.IterateOnRange(&tree.Range{Min: testutil.NewKey(t, pivot...)}, false, func(key *tree.Key) error {
			if i == 0 {
				require.Equal(t, "other-key", string(key.Encoded))
			} else if i == 1 {
				require.Equal(t, "yet-another-key", string(key.Encoded))
			} else {
				return errors.New("should not reach this point")
			}

			i++
			return nil
		})
		assert.NoError(t, err)
		require.Equal(t, 2, i)
	})

	t.Run("Delete valid key succeeds (arity=2)", func(t *testing.T) {
		idx := getIndex(t, 2)

		assert.NoError(t, idx.Set(values(types.NewDoubleValue(10), types.NewDoubleValue(10)), []byte("key")))
		assert.NoError(t, idx.Set(values(types.NewIntegerValue(10), types.NewIntegerValue(10)), []byte("other-key")))
		assert.NoError(t, idx.Set(values(types.NewIntegerValue(11), types.NewIntegerValue(11)), []byte("yet-another-key")))
		assert.NoError(t, idx.Set(values(types.NewTextValue("hello"), types.NewTextValue("hello")), []byte("yet-another-different-key")))
		assert.NoError(t, idx.Delete(values(types.NewDoubleValue(10), types.NewDoubleValue(10)), []byte("key")))

		pivot := values(types.NewIntegerValue(10))
		i := 0
		err := idx.IterateOnRange(&tree.Range{Min: testutil.NewKey(t, pivot...)}, false, func(key *tree.Key) error {
			if i == 0 {
				require.Equal(t, "other-key", string(key.Encoded))
			} else if i == 1 {
				require.Equal(t, "yet-another-key", string(key.Encoded))
			} else {
				return errors.New("should not reach this point")
			}

			i++
			return nil
		})
		assert.NoError(t, err)
		require.Equal(t, 2, i)
	})

	t.Run("Delete non existing key fails", func(t *testing.T) {
		idx := getIndex(t, 1)

		assert.Error(t, idx.Delete(values(types.NewTextValue("foo")), []byte("foo")))
	})
}

func TestIndexExists(t *testing.T) {
	idx := getIndex(t, 2)

	assert.NoError(t, idx.Set(values(types.NewDoubleValue(10), types.NewIntegerValue(11)), []byte("key1")))
	assert.NoError(t, idx.Set(values(types.NewDoubleValue(10), types.NewIntegerValue(12)), []byte("key2")))

	ok, key, err := idx.Exists(values(types.NewDoubleValue(10), types.NewIntegerValue(11)))
	assert.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, tree.NewEncodedKey([]byte("key1")), key)

	ok, _, err = idx.Exists(values(types.NewDoubleValue(11), types.NewIntegerValue(11)))
	assert.NoError(t, err)
	require.False(t, ok)
}

// BenchmarkIndexSet benchmarks the Set method with 1, 10, 1000 and 10000 successive insertions.
func BenchmarkIndexSet(b *testing.B) {
	for size := 10; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			b.ResetTimer()
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				idx := getIndex(b, 1)

				b.StartTimer()
				for j := 0; j < size; j++ {
					k := fmt.Sprintf("name-%d", j)
					_ = idx.Set(values(types.NewTextValue(k)), []byte(k))
				}
				b.StopTimer()
			}
		})
	}
}

// BenchmarkIndexIteration benchmarks the iterarion of a cursor with 1, 10, 1000 and 10000 items.
func BenchmarkIndexIteration(b *testing.B) {
	for size := 10; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			idx := getIndex(b, 1)

			for i := 0; i < size; i++ {
				k := []byte(fmt.Sprintf("name-%d", i))
				_ = idx.Set(values(types.NewTextValue(string(k))), k)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = idx.IterateOnRange(&tree.Range{Min: testutil.NewKey(b, types.NewTextValue(""))}, false, func(_ *tree.Key) error {
					return nil
				})
			}
			b.StopTimer()
		})
	}
}

// BenchmarkCompositeIndexSet benchmarks the Set method with 1, 10, 1000 and 10000 successive insertions.
func BenchmarkCompositeIndexSet(b *testing.B) {
	for size := 10; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			b.ResetTimer()
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				idx := getIndex(b, 2)

				b.StartTimer()
				for j := 0; j < size; j++ {
					k := fmt.Sprintf("name-%d", j)
					_ = idx.Set(values(types.NewTextValue(k), types.NewTextValue(k)), []byte(k))
				}
				b.StopTimer()
			}
		})
	}
}

// BenchmarkCompositeIndexIteration benchmarks the iterarion of a cursor with 1, 10, 1000 and 10000 items.
func BenchmarkCompositeIndexIteration(b *testing.B) {
	for size := 10; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			idx := getIndex(b, 2)

			for i := 0; i < size; i++ {
				k := []byte(fmt.Sprintf("name-%d", i))
				_ = idx.Set(values(types.NewTextValue(string(k)), types.NewTextValue(string(k))), k)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = idx.IterateOnRange(&tree.Range{Min: testutil.NewKey(b, types.NewTextValue(""), types.NewTextValue(""))}, false, func(_ *tree.Key) error {
					return nil
				})
			}
			b.StopTimer()
		})
	}
}
