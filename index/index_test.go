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

func getIndex(t testing.TB, opts index.Options) (index.Index, func()) {
	ng := memory.NewEngine()
	tx, err := ng.Begin(true)
	require.NoError(t, err)

	err = tx.CreateStore("test")
	require.NoError(t, err)

	st, err := tx.Store("test")
	require.NoError(t, err)

	return index.New(st, opts), func() {
		tx.Rollback()
	}
}

func TestIndexSet(t *testing.T) {
	for _, unique := range []bool{true, false} {
		text := fmt.Sprintf("Unique: %v, ", unique)

		t.Run(text+"Set nil value fails", func(t *testing.T) {
			idx, cleanup := getIndex(t, index.Options{Unique: unique})
			defer cleanup()
			require.Error(t, idx.Set(nil, []byte("rid")))
			require.Error(t, idx.Set([]byte{}, []byte("rid")))
		})

		t.Run(text+"Set nil key succeeds", func(t *testing.T) {
			idx, cleanup := getIndex(t, index.Options{Unique: unique})
			defer cleanup()
			require.NoError(t, idx.Set([]byte("value"), nil))
		})

		t.Run(text+"Set value and key succeeds", func(t *testing.T) {
			idx, cleanup := getIndex(t, index.Options{Unique: unique})
			defer cleanup()
			require.NoError(t, idx.Set([]byte("value"), []byte("key")))
		})
	}

	t.Run("Unique: true, Duplicate", func(t *testing.T) {
		idx, cleanup := getIndex(t, index.Options{Unique: true})
		defer cleanup()

		require.NoError(t, idx.Set([]byte("value1"), []byte("key")))
		require.NoError(t, idx.Set([]byte("value2"), []byte("key")))
		require.Equal(t, index.ErrDuplicate, idx.Set([]byte("value1"), []byte("key")))
	})
}

func TestIndexDelete(t *testing.T) {
	t.Run("Unique: false, Delete valid key succeeds", func(t *testing.T) {
		idx, cleanup := getIndex(t, index.Options{Unique: false})
		defer cleanup()

		require.NoError(t, idx.Set([]byte("value1"), []byte("key")))
		require.NoError(t, idx.Set([]byte("value1"), []byte("other-key")))
		require.NoError(t, idx.Set([]byte("value2"), []byte("yet-another-key")))
		require.NoError(t, idx.Delete([]byte("key")))

		i := 0
		err := idx.AscendGreaterOrEqual([]byte("value1"), func(v, key []byte) error {
			if i == 0 {
				require.Equal(t, "value1", string(v))
				require.Equal(t, "other-key", string(key))
			} else if i == 1 {
				require.Equal(t, "value2", string(v))
				require.Equal(t, "yet-another-key", string(key))
			} else {
				return errors.New("should not reach this point")
			}

			i++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, i)
	})

	t.Run("Unique: true, Delete valid key succeeds", func(t *testing.T) {
		idx, cleanup := getIndex(t, index.Options{Unique: true})
		defer cleanup()

		require.NoError(t, idx.Set([]byte("value1"), []byte("key1")))
		require.NoError(t, idx.Set([]byte("value2"), []byte("key1")))
		require.NoError(t, idx.Set([]byte("value3"), []byte("key2")))
		require.NoError(t, idx.Delete([]byte("key1")))

		i := 0
		err := idx.AscendGreaterOrEqual(nil, func(v, key []byte) error {
			if i == 0 {
				require.Equal(t, "value3", string(v))
				require.Equal(t, "key2", string(key))
			} else {
				return errors.New("should not reach this point")
			}

			i++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 1, i)
	})

	for _, unique := range []bool{true, false} {
		text := fmt.Sprintf("Unique: %v, ", unique)

		t.Run(text+"Delete non existing key succeeds", func(t *testing.T) {
			idx, cleanup := getIndex(t, index.Options{Unique: unique})
			defer cleanup()

			require.NoError(t, idx.Delete([]byte("foo")))
		})
	}
}

func TestIndexAscendGreaterThan(t *testing.T) {
	for _, unique := range []bool{true, false} {
		text := fmt.Sprintf("Unique: %v, ", unique)

		t.Run(text+"Should not iterate if index is empty", func(t *testing.T) {
			idx, cleanup := getIndex(t, index.Options{Unique: unique})
			defer cleanup()

			i := 0
			err := idx.AscendGreaterOrEqual(nil, func(value []byte, key []byte) error {
				i++
				return errors.New("should not iterate")
			})
			require.NoError(t, err)
			require.Equal(t, 0, i)
		})

		t.Run(text+"With no pivot, should iterate over all records in order", func(t *testing.T) {
			idx, cleanup := getIndex(t, index.Options{Unique: unique})
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

		t.Run(text+"With pivot, should iterate over some records in order", func(t *testing.T) {
			idx, cleanup := getIndex(t, index.Options{Unique: unique})
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

}

func TestIndexDescendLessOrEqual(t *testing.T) {
	for _, unique := range []bool{true, false} {
		text := fmt.Sprintf("Unique: %v, ", unique)

		t.Run(text+"Should not iterate if index is empty", func(t *testing.T) {
			idx, cleanup := getIndex(t, index.Options{Unique: unique})
			defer cleanup()

			i := 0
			err := idx.DescendLessOrEqual(nil, func(value []byte, key []byte) error {
				i++
				return errors.New("should not iterate")
			})
			require.NoError(t, err)
			require.Equal(t, 0, i)
		})

		t.Run(text+"With no pivot, should iterate over all records in reverse order", func(t *testing.T) {
			idx, cleanup := getIndex(t, index.Options{Unique: unique})
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

		t.Run(text+"With pivot, should iterate over some records in order", func(t *testing.T) {
			idx, cleanup := getIndex(t, index.Options{Unique: unique})
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
}

// BenchmarkIndexSet benchmarks the Set method with 1, 10, 1000 and 10000 successive insertions.
func BenchmarkIndexSet(b *testing.B) {
	for size := 10; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {

			b.ResetTimer()
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				idx, cleanup := getIndex(b, index.Options{})

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
			idx, cleanup := getIndex(b, index.Options{})
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
