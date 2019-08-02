// Package indextest defines a list of tests that can be used to test index implementations.
package index_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/index"
	"github.com/stretchr/testify/require"
)

func getIndex(t testing.TB) (*index.Index, func()) {
	ng := memory.NewEngine()
	tx, err := ng.Begin(true)
	require.NoError(t, err)

	err = tx.CreateStore("test")
	require.NoError(t, err)

	st, err := tx.Store("test")
	require.NoError(t, err)

	return &index.Index{Store: st}, func() {
		tx.Rollback()
	}
}

func TestIndexSet(t *testing.T) {
	idx, cleanup := getIndex(t)
	defer cleanup()

	t.Run("Set nil value fails", func(t *testing.T) {
		require.Error(t, idx.Set(nil, []byte("rid")))
		require.Error(t, idx.Set([]byte{}, []byte("rid")))
	})

	t.Run("Set nil recordID succeeds", func(t *testing.T) {
		require.NoError(t, idx.Set([]byte("value"), nil))
	})

	t.Run("Set value and recordID succeeds", func(t *testing.T) {
		require.NoError(t, idx.Set([]byte("value"), []byte("recordID")))
	})
}

func TestIndexDelete(t *testing.T) {
	idx, cleanup := getIndex(t)
	defer cleanup()

	t.Run("Delete valid recordID succeeds", func(t *testing.T) {
		require.NoError(t, idx.Set([]byte("value1"), []byte("recordID")))
		require.NoError(t, idx.Set([]byte("value1"), []byte("other-recordID")))
		require.NoError(t, idx.Set([]byte("value2"), []byte("yet-another-recordID")))
		require.NoError(t, idx.Delete([]byte("recordID")))

		i := 0
		err := idx.AscendGreaterOrEqual([]byte("value1"), func(v, recordID []byte) error {
			if i == 0 {
				require.Equal(t, "value1", string(v))
				require.Equal(t, "other-recordID", string(recordID))
			} else if i == 1 {
				require.Equal(t, "value2", string(v))
				require.Equal(t, "yet-another-recordID", string(recordID))
			} else {
				return errors.New("should not reach this point")
			}

			i++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, i)
	})

	t.Run("Delete non existing recordID succeeds", func(t *testing.T) {
		require.NoError(t, idx.Delete([]byte("foo")))
	})
}

func TestIndexAscendGreaterThan(t *testing.T) {
	t.Run("Should not iterate if index is empty", func(t *testing.T) {
		idx, cleanup := getIndex(t)
		defer cleanup()

		i := 0
		err := idx.AscendGreaterOrEqual(nil, func(value []byte, recordID []byte) error {
			i++
			return errors.New("should not iterate")
		})
		require.NoError(t, err)
		require.Equal(t, 0, i)
	})

	t.Run("With no pivot, should iterate over all records in order", func(t *testing.T) {
		idx, cleanup := getIndex(t)
		defer cleanup()

		for i := byte(0); i < 10; i += 2 {
			require.NoError(t, idx.Set([]byte{'A' + i}, []byte{'a' + i}))
		}

		var i uint8
		var count int
		err := idx.AscendGreaterOrEqual(nil, func(v, rid []byte) error {
			require.Equal(t, []byte{'A' + i}, v)
			require.Equal(t, []byte{'a' + i}, rid)

			i += 2
			count++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 5, count)
	})

	t.Run("With pivot, should iterate over some records in order", func(t *testing.T) {
		idx, cleanup := getIndex(t)
		defer cleanup()

		for i := byte(0); i < 10; i += 2 {
			require.NoError(t, idx.Set([]byte{'A' + i}, []byte{'a' + i}))
		}

		var i uint8
		var count int
		err := idx.AscendGreaterOrEqual([]byte{'C'}, func(v, rid []byte) error {
			require.Equal(t, []byte{'C' + i}, v)
			require.Equal(t, []byte{'c' + i}, rid)

			i += 2
			count++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 4, count)
	})
}

func TestIndexDescendLessOrEqual(t *testing.T) {
	t.Run("Should not iterate if index is empty", func(t *testing.T) {
		idx, cleanup := getIndex(t)
		defer cleanup()

		i := 0
		err := idx.DescendLessOrEqual(nil, func(value []byte, recordID []byte) error {
			i++
			return errors.New("should not iterate")
		})
		require.NoError(t, err)
		require.Equal(t, 0, i)
	})

	t.Run("With no pivot, should iterate over all records in reverse order", func(t *testing.T) {
		idx, cleanup := getIndex(t)
		defer cleanup()

		for i := byte(0); i < 10; i += 2 {
			require.NoError(t, idx.Set([]byte{'A' + i}, []byte{'a' + i}))
		}

		var i uint8 = 8
		var count int
		err := idx.DescendLessOrEqual(nil, func(v, rid []byte) error {
			require.Equal(t, []byte{'A' + i}, v)
			require.Equal(t, []byte{'a' + i}, rid)

			i -= 2
			count++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 5, count)
	})

	t.Run("With pivot, should iterate over some records in order", func(t *testing.T) {
		idx, cleanup := getIndex(t)
		defer cleanup()

		for i := byte(0); i < 10; i++ {
			require.NoError(t, idx.Set([]byte{'A' + i}, []byte{'a' + i}))
		}

		var i byte = 0
		var count int
		err := idx.DescendLessOrEqual([]byte{'F'}, func(v, rid []byte) error {
			require.Equal(t, []byte{'F' - i}, v)
			require.Equal(t, []byte{'f' - i}, rid)

			i++
			count++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 6, count)
	})
}

// BenchmarkIndexSet benchmarks the Set method with 1, 10, 1000 and 10000 successive insertions.
func BenchmarkIndexSet(b *testing.B) {
	for size := 10; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {

			b.ResetTimer()
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				idx, cleanup := getIndex(b)

				b.StartTimer()
				for j := 0; j < size; j++ {
					k := []byte(fmt.Sprintf("name-%d", j))
					idx.Set(k, k)
				}
				b.StopTimer()
				cleanup()
			}
		})
	}
}

// BenchmarkIndexIteration benchmarks the iterarion of a cursor with 1, 10, 1000 and 10000 items.
func BenchmarkIndexIteration(b *testing.B) {
	for size := 10; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			idx, cleanup := getIndex(b)
			defer cleanup()

			for i := 0; i < size; i++ {
				k := []byte(fmt.Sprintf("name-%d", i))
				idx.Set(k, k)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx.AscendGreaterOrEqual(nil, func(_, _ []byte) error {
					return nil
				})
			}
			b.StopTimer()
		})
	}
}
