// Package indextest defines a list of tests that can be used to test index implementations.
package index_test

import (
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/asdine/genji/engine/memoryengine"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/document"
	"github.com/stretchr/testify/require"
)

func getIndex(t testing.TB, opts index.Options) (index.Index, func()) {
	ng := memoryengine.NewEngine()
	tx, err := ng.Begin(true)
	require.NoError(t, err)

	return index.New(tx, opts), func() {
		tx.Rollback()
	}
}

func TestIndexSet(t *testing.T) {
	for _, unique := range []bool{true, false} {
		text := fmt.Sprintf("Unique: %v, ", unique)

		t.Run(text+"Set nil key succeeds", func(t *testing.T) {
			idx, cleanup := getIndex(t, index.Options{Unique: unique})
			defer cleanup()
			require.NoError(t, idx.Set(document.NewBoolValue(true), nil))
		})

		t.Run(text+"Set value and key succeeds", func(t *testing.T) {
			idx, cleanup := getIndex(t, index.Options{Unique: unique})
			defer cleanup()
			require.NoError(t, idx.Set(document.NewBoolValue(true), []byte("key")))
		})
	}

	t.Run("Unique: true, Duplicate", func(t *testing.T) {
		idx, cleanup := getIndex(t, index.Options{Unique: true})
		defer cleanup()

		require.NoError(t, idx.Set(document.NewIntValue(10), []byte("key")))
		require.NoError(t, idx.Set(document.NewIntValue(11), []byte("key")))
		require.Equal(t, index.ErrDuplicate, idx.Set(document.NewIntValue(10), []byte("key")))
	})
}

func TestIndexDelete(t *testing.T) {
	t.Run("Unique: false, Delete valid key succeeds", func(t *testing.T) {
		idx, cleanup := getIndex(t, index.Options{Unique: false})
		defer cleanup()

		require.NoError(t, idx.Set(document.NewIntValue(10), []byte("key")))
		require.NoError(t, idx.Set(document.NewIntValue(10), []byte("other-key")))
		require.NoError(t, idx.Set(document.NewIntValue(11), []byte("yet-another-key")))
		require.NoError(t, idx.Delete(document.NewIntValue(10), []byte("key")))

		pivot := document.NewIntValue(10)
		i := 0
		err := idx.AscendGreaterOrEqual(&pivot, func(val document.Value, key []byte) error {
			if i == 0 {
				require.Equal(t, document.NewFloat64Value(10), val)
				require.Equal(t, "other-key", string(key))
			} else if i == 1 {
				require.Equal(t, document.NewFloat64Value(11), val)
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

		require.NoError(t, idx.Set(document.NewIntValue(10), []byte("key1")))
		require.NoError(t, idx.Set(document.NewIntValue(11), []byte("key2")))
		require.NoError(t, idx.Set(document.NewIntValue(12), []byte("key3")))
		require.NoError(t, idx.Delete(document.NewIntValue(11), []byte("key2")))

		i := 0
		err := idx.AscendGreaterOrEqual(index.EmptyPivot(document.IntValue), func(val document.Value, key []byte) error {
			switch i {
			case 0:
				require.Equal(t, document.NewFloat64Value(10), val)
				require.Equal(t, "key1", string(key))
			case 1:
				require.Equal(t, document.NewFloat64Value(12), val)
				require.Equal(t, "key3", string(key))
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
			idx, cleanup := getIndex(t, index.Options{Unique: unique})
			defer cleanup()

			require.Error(t, idx.Delete(document.NewStringValue("foo"), []byte("foo")))
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
			err := idx.AscendGreaterOrEqual(index.EmptyPivot(document.Int32Value), func(val document.Value, key []byte) error {
				i++
				return errors.New("should not iterate")
			})
			require.NoError(t, err)
			require.Equal(t, 0, i)
		})

		t.Run(text+"With typed empty pivot, should iterate over all records of the pivot type in order", func(t *testing.T) {
			tests := []struct {
				name  string
				val   func(i int) document.Value
				t     index.Type
				pivot *document.Value
			}{
				{"floats", func(i int) document.Value { return document.NewInt32Value(int32(i)) }, index.Float, index.EmptyPivot(document.Int32Value)},
				{"bytes", func(i int) document.Value { return document.NewStringValue(string([]byte{byte(i)})) }, index.Bytes, index.EmptyPivot(document.StringValue)},
			}

			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					idx, cleanup := getIndex(t, index.Options{Unique: unique})
					defer cleanup()

					for i := 0; i < 10; i += 2 {
						require.NoError(t, idx.Set(test.val(i), []byte{'a' + byte(i)}))
					}

					var i uint8
					var count int
					err := idx.AscendGreaterOrEqual(test.pivot, func(val document.Value, rid []byte) error {
						switch test.t {
						case index.Float:
							require.Equal(t, document.NewFloat64Value(float64(i)), val)
						case index.Bytes:
							require.Equal(t, document.NewBytesValue([]byte{i}), val)
						case index.Bool:
							require.Equal(t, document.NewBoolValue(i > 0), val)
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

		t.Run(text+"With pivot, should iterate over some records in order", func(t *testing.T) {
			idx, cleanup := getIndex(t, index.Options{Unique: unique})
			defer cleanup()

			for i := byte(0); i < 10; i += 2 {
				require.NoError(t, idx.Set(document.NewStringValue(string([]byte{'A' + i})), []byte{'a' + i}))
			}

			var i uint8
			var count int
			pivot := document.NewStringValue("C")
			err := idx.AscendGreaterOrEqual(&pivot, func(val document.Value, rid []byte) error {
				require.Equal(t, document.NewBytesValue([]byte{'C' + i}), val)
				require.Equal(t, []byte{'c' + i}, rid)

				i += 2
				count++
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, 4, count)
		})

		t.Run(text+"With no pivot, should iterate over all records in order, regardless of their type", func(t *testing.T) {
			idx, cleanup := getIndex(t, index.Options{Unique: unique})
			defer cleanup()

			for i := 0; i < 10; i++ {
				require.NoError(t, idx.Set(document.NewIntValue(i), []byte{'i', 'a' + byte(i)}))
				require.NoError(t, idx.Set(document.NewStringValue(strconv.Itoa(i)), []byte{'s', 'a' + byte(i)}))
			}

			var floats, bytes int
			var count int
			err := idx.AscendGreaterOrEqual(nil, func(val document.Value, rid []byte) error {
				switch val.Type {
				case document.Float64Value:
					require.Equal(t, document.NewFloat64Value(float64(floats)), val)
					require.Equal(t, []byte{'i', 'a' + byte(floats)}, rid)
					floats++
				case document.BytesValue:
					require.Equal(t, document.NewBytesValue([]byte(strconv.Itoa(int(bytes)))), val)
					require.Equal(t, []byte{'s', 'a' + byte(bytes)}, rid)
					bytes++
				default:
					t.FailNow()
				}

				count++
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, 20, count)
			require.Equal(t, 10, floats)
			require.Equal(t, 10, bytes)
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
			err := idx.DescendLessOrEqual(index.EmptyPivot(document.Int32Value), func(val document.Value, key []byte) error {
				i++
				return errors.New("should not iterate")
			})
			require.NoError(t, err)
			require.Equal(t, 0, i)
		})

		t.Run(text+"With empty typed pivot, should iterate over all records of the same type in reverse order", func(t *testing.T) {
			idx, cleanup := getIndex(t, index.Options{Unique: unique})
			defer cleanup()

			for i := byte(0); i < 10; i += 2 {
				require.NoError(t, idx.Set(document.NewInt32Value(int32(i)), []byte{'a' + i}))
			}

			var i uint8 = 8
			var count int
			err := idx.DescendLessOrEqual(index.EmptyPivot(document.Int32Value), func(val document.Value, rid []byte) error {
				require.Equal(t, document.NewFloat64Value(float64(i)), val)
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
				require.NoError(t, idx.Set(document.NewStringValue(string([]byte{'A' + i})), []byte{'a' + i}))
			}

			var i byte = 0
			var count int
			pivot := document.NewStringValue("F")
			err := idx.DescendLessOrEqual(&pivot, func(val document.Value, rid []byte) error {
				require.Equal(t, document.NewBytesValue([]byte{'F' - i}), val)
				require.Equal(t, []byte{'f' - i}, rid)

				i++
				count++
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, 6, count)
		})

		t.Run(text+"With no pivot, should iterate over all records in reverse order, regardless of their type", func(t *testing.T) {
			idx, cleanup := getIndex(t, index.Options{Unique: unique})
			defer cleanup()

			for i := 0; i < 10; i++ {
				require.NoError(t, idx.Set(document.NewIntValue(i), []byte{'i', 'a' + byte(i)}))
				require.NoError(t, idx.Set(document.NewStringValue(strconv.Itoa(i)), []byte{'s', 'a' + byte(i)}))
			}

			var floats, bytes int = 9, 9
			var count int = 20
			err := idx.DescendLessOrEqual(nil, func(val document.Value, rid []byte) error {
				switch val.Type {
				case document.Float64Value:
					require.Equal(t, document.NewFloat64Value(float64(floats)), val)
					require.Equal(t, []byte{'i', 'a' + byte(floats)}, rid)
					floats--
				case document.BytesValue:
					require.Equal(t, document.NewBytesValue([]byte(strconv.Itoa(int(bytes)))), val)
					require.Equal(t, []byte{'s', 'a' + byte(bytes)}, rid)
					bytes--
				default:
					t.FailNow()
				}

				count--
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, 0, count)
			require.Equal(t, -1, floats)
			require.Equal(t, -1, bytes)
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
					idx.Set(document.NewBytesValue(k), k)
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
				idx.Set(document.NewStringValue(string(k)), k)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx.AscendGreaterOrEqual(index.EmptyPivot(document.StringValue), func(_ document.Value, _ []byte) error {
					return nil
				})
			}
			b.StopTimer()
		})
	}
}
