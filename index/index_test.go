// Package indextest defines a list of tests that can be used to test index implementations.
package index_test

import (
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine/memoryengine"
	"github.com/genjidb/genji/index"
	"github.com/genjidb/genji/key"
	"github.com/stretchr/testify/require"
)

func getIndex(t testing.TB, unique bool) (index.Index, func()) {
	ng := memoryengine.NewEngine()
	tx, err := ng.Begin(true)
	require.NoError(t, err)

	var idx index.Index

	if unique {
		idx = index.NewUniqueIndex(tx, "foo")
	} else {
		idx = index.NewListIndex(tx, "foo")
	}

	return idx, func() {
		tx.Rollback()
	}
}

func TestIndexSet(t *testing.T) {
	for _, unique := range []bool{true, false} {
		text := fmt.Sprintf("Unique: %v, ", unique)

		t.Run(text+"Set nil key succeeds", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique)
			defer cleanup()
			require.NoError(t, idx.Set(document.NewBoolValue(true), nil))
		})

		t.Run(text+"Set value and key succeeds", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique)
			defer cleanup()
			require.NoError(t, idx.Set(document.NewBoolValue(true), []byte("key")))
		})
	}

	t.Run("Unique: true, Duplicate", func(t *testing.T) {
		idx, cleanup := getIndex(t, true)
		defer cleanup()

		require.NoError(t, idx.Set(document.NewIntegerValue(10), []byte("key")))
		require.NoError(t, idx.Set(document.NewIntegerValue(11), []byte("key")))
		require.Equal(t, index.ErrDuplicate, idx.Set(document.NewIntegerValue(10), []byte("key")))
	})
}

func TestIndexDelete(t *testing.T) {
	t.Run("Unique: false, Delete valid key succeeds", func(t *testing.T) {
		idx, cleanup := getIndex(t, false)
		defer cleanup()

		require.NoError(t, idx.Set(document.NewIntegerValue(10), []byte("key")))
		require.NoError(t, idx.Set(document.NewIntegerValue(10), []byte("other-key")))
		require.NoError(t, idx.Set(document.NewIntegerValue(11), []byte("yet-another-key")))
		require.NoError(t, idx.Delete(document.NewIntegerValue(10), []byte("key")))

		pivot := document.NewIntegerValue(10)
		i := 0
		err := idx.AscendGreaterOrEqual(pivot, func(v, k []byte, isEqual bool) error {
			if i == 0 {
				require.True(t, isEqual)
				requireEqualEncoded(t, document.NewDoubleValue(10), v)
				require.Equal(t, "other-key", string(k))
			} else if i == 1 {
				require.False(t, isEqual)
				requireEqualEncoded(t, document.NewDoubleValue(11), v)
				require.Equal(t, "yet-another-key", string(k))
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
		idx, cleanup := getIndex(t, true)
		defer cleanup()

		require.NoError(t, idx.Set(document.NewIntegerValue(10), []byte("key1")))
		require.NoError(t, idx.Set(document.NewIntegerValue(11), []byte("key2")))
		require.NoError(t, idx.Set(document.NewIntegerValue(12), []byte("key3")))
		require.NoError(t, idx.Delete(document.NewIntegerValue(11), []byte("key2")))

		i := 0
		err := idx.AscendGreaterOrEqual(document.Value{Type: document.IntegerValue}, func(v, k []byte, isEqual bool) error {
			switch i {
			case 0:
				require.False(t, isEqual)
				requireEqualEncoded(t, document.NewDoubleValue(10), v)
				require.Equal(t, "key1", string(k))
			case 1:
				require.False(t, isEqual)
				requireEqualEncoded(t, document.NewDoubleValue(12), v)
				require.Equal(t, "key3", string(k))
			default:
				return errors.New("should not reach this point")
			}

			i++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, i)
	})

	for _, unique := range []bool{true, false} {
		text := fmt.Sprintf("Unique: %v, ", unique)

		t.Run(text+"Delete non existing key fails", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique)
			defer cleanup()

			require.Error(t, idx.Delete(document.NewTextValue("foo"), []byte("foo")))
		})
	}
}

func requireEqualEncoded(t *testing.T, expected document.Value, actual []byte) {
	t.Helper()

	enc := key.AppendValue(nil, expected)
	require.Equal(t, enc, actual)
}

func TestIndexAscendGreaterThan(t *testing.T) {
	for _, unique := range []bool{true, false} {
		text := fmt.Sprintf("Unique: %v, ", unique)

		t.Run(text+"Should not iterate if index is empty", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique)
			defer cleanup()

			i := 0
			err := idx.AscendGreaterOrEqual(document.Value{Type: document.IntegerValue}, func(val, key []byte, isEqual bool) error {
				i++
				return errors.New("should not iterate")
			})
			require.NoError(t, err)
			require.Equal(t, 0, i)
		})

		t.Run(text+"With typed empty pivot, should iterate over all documents of the pivot type in order", func(t *testing.T) {
			tests := []struct {
				name  string
				val   func(i int) document.Value
				t     document.ValueType
				pivot document.Value
			}{
				{"integers", func(i int) document.Value { return document.NewIntegerValue(int64(i)) }, document.IntegerValue, document.Value{Type: document.IntegerValue}},
				{"texts", func(i int) document.Value { return document.NewTextValue(strconv.Itoa(i)) }, document.TextValue, document.Value{Type: document.TextValue}},
			}

			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					idx, cleanup := getIndex(t, unique)
					defer cleanup()

					for i := 0; i < 10; i += 2 {
						require.NoError(t, idx.Set(test.val(i), []byte{'a' + byte(i)}))
					}

					var i uint8
					var count int
					err := idx.AscendGreaterOrEqual(test.pivot, func(val, rid []byte, isEqual bool) error {
						switch test.t {
						case document.IntegerValue:
							requireEqualEncoded(t, document.NewDoubleValue(float64(i)), val)
						case document.TextValue:
							requireEqualEncoded(t, document.NewTextValue(strconv.Itoa(int(i))), val)
						case document.BoolValue:
							requireEqualEncoded(t, document.NewBoolValue(i > 0), val)
						}
						require.Equal(t, []byte{'a' + i}, rid)

						i += 2
						count++
						return nil
					})
					require.NoError(t, err)
					require.Equal(t, 5, count)
				})
			}
		})

		t.Run(text+"With pivot, should iterate over some documents in order", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique)
			defer cleanup()

			for i := byte(0); i < 10; i += 2 {
				require.NoError(t, idx.Set(document.NewTextValue(string([]byte{'A' + i})), []byte{'a' + i}))
			}

			var i uint8
			var count int
			pivot := document.NewTextValue("C")
			err := idx.AscendGreaterOrEqual(pivot, func(val, rid []byte, isEqual bool) error {
				requireEqualEncoded(t, document.NewTextValue(string([]byte{'C' + i})), val)
				require.Equal(t, []byte{'c' + i}, rid)

				i += 2
				count++
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, 4, count)
		})

		t.Run(text+"With no pivot, should iterate over all documents in order, regardless of their type", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique)
			defer cleanup()

			for i := int64(0); i < 10; i++ {
				require.NoError(t, idx.Set(document.NewIntegerValue(i), []byte{'i', 'a' + byte(i)}))
				require.NoError(t, idx.Set(document.NewTextValue(strconv.Itoa(int(i))), []byte{'s', 'a' + byte(i)}))
			}

			var ints, texts int
			var count int
			err := idx.AscendGreaterOrEqual(document.Value{}, func(val, rid []byte, isEqual bool) error {
				if count < 10 {
					requireEqualEncoded(t, document.NewDoubleValue(float64(ints)), val)
					require.Equal(t, []byte{'i', 'a' + byte(ints)}, rid)
					ints++
				} else {
					requireEqualEncoded(t, document.NewTextValue(strconv.Itoa(int(texts))), val)
					require.Equal(t, []byte{'s', 'a' + byte(texts)}, rid)
					texts++
				}
				count++
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, 20, count)
			require.Equal(t, 10, ints)
			require.Equal(t, 10, texts)
		})
	}
}

func TestIndexDescendLessOrEqual(t *testing.T) {
	for _, unique := range []bool{true, false} {
		text := fmt.Sprintf("Unique: %v, ", unique)

		t.Run(text+"Should not iterate if index is empty", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique)
			defer cleanup()

			i := 0
			err := idx.DescendLessOrEqual(document.Value{Type: document.IntegerValue}, func(val, key []byte, isEqual bool) error {
				i++
				return errors.New("should not iterate")
			})
			require.NoError(t, err)
			require.Equal(t, 0, i)
		})

		t.Run(text+"With empty typed pivot, should iterate over all documents of the same type in reverse order", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique)
			defer cleanup()

			for i := byte(0); i < 10; i += 2 {
				require.NoError(t, idx.Set(document.NewIntegerValue(int64(i)), []byte{'a' + i}))
			}

			var i uint8 = 8
			var count int
			err := idx.DescendLessOrEqual(document.Value{Type: document.IntegerValue}, func(val, key []byte, isEqual bool) error {
				requireEqualEncoded(t, document.NewDoubleValue(float64(i)), val)
				require.Equal(t, []byte{'a' + i}, key)

				i -= 2
				count++
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, 5, count)
		})

		t.Run(text+"With pivot, should iterate over some documents in order", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique)
			defer cleanup()

			for i := byte(0); i < 10; i++ {
				require.NoError(t, idx.Set(document.NewTextValue(string([]byte{'A' + i})), []byte{'a' + i}))
			}

			var i byte = 0
			var count int
			pivot := document.NewTextValue("F")
			err := idx.DescendLessOrEqual(pivot, func(val, rid []byte, isEqual bool) error {
				requireEqualEncoded(t, document.NewTextValue(string([]byte{'F' - i})), val)
				require.Equal(t, []byte{'f' - i}, rid)

				i++
				count++
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, 6, count)
		})

		t.Run(text+"With no pivot, should iterate over all documents in reverse order, regardless of their type", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique)
			defer cleanup()

			for i := 0; i < 10; i++ {
				require.NoError(t, idx.Set(document.NewIntegerValue(int64(i)), []byte{'i', 'a' + byte(i)}))
				require.NoError(t, idx.Set(document.NewTextValue(strconv.Itoa(i)), []byte{'s', 'a' + byte(i)}))
			}

			var ints, texts int = 9, 9
			var count int = 20
			err := idx.DescendLessOrEqual(document.Value{}, func(val, rid []byte, isEqual bool) error {
				if count > 10 {
					requireEqualEncoded(t, document.NewTextValue(strconv.Itoa(int(texts))), val)
					require.Equal(t, []byte{'s', 'a' + byte(texts)}, rid)
					texts--
				} else {
					requireEqualEncoded(t, document.NewDoubleValue(float64(ints)), val)
					require.Equal(t, []byte{'i', 'a' + byte(ints)}, rid)
					ints--
				}

				count--
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, 0, count)
			require.Equal(t, -1, ints)
			require.Equal(t, -1, texts)
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
				idx, cleanup := getIndex(b, false)

				b.StartTimer()
				for j := 0; j < size; j++ {
					k := fmt.Sprintf("name-%d", j)
					idx.Set(document.NewTextValue(k), []byte(k))
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
			idx, cleanup := getIndex(b, false)
			defer cleanup()

			for i := 0; i < size; i++ {
				k := []byte(fmt.Sprintf("name-%d", i))
				idx.Set(document.NewTextValue(string(k)), k)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx.AscendGreaterOrEqual(document.Value{Type: document.TextValue}, func(_, _ []byte, isEqual bool) error {
					return nil
				})
			}
			b.StopTimer()
		})
	}
}
