package database_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/kv"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

// values is a helper function to avoid having to type []types.Value{} all the time.
func values(vs ...types.Value) []types.Value {
	return vs
}

func getIndex(t testing.TB, arity int) (*database.Index, func()) {
	ng := testutil.NewEngine(t)
	tx, err := ng.Begin(context.Background(), kv.TxOptions{
		Writable: true,
	})
	assert.NoError(t, err)

	err = tx.CreateStore([]byte("foo"))
	assert.NoError(t, err)
	st, err := tx.GetStore([]byte("foo"))
	assert.NoError(t, err)
	tr := tree.New(st)

	var paths []document.Path
	for i := 0; i < arity; i++ {
		paths = append(paths, document.NewPath(fmt.Sprintf("[%d]", i)))
	}
	idx := database.NewIndex(tr, database.IndexInfo{Paths: paths})

	return idx, func() {
		tx.Rollback()
	}
}

func TestIndexSet(t *testing.T) {
	t.Run("Set nil key falls (arity=1)", func(t *testing.T) {
		idx, cleanup := getIndex(t, 1)
		defer cleanup()
		assert.Error(t, idx.Set(values(types.NewBoolValue(true)), nil))
	})

	t.Run("Set value and key succeeds (arity=1)", func(t *testing.T) {
		idx, cleanup := getIndex(t, 1)
		defer cleanup()
		assert.NoError(t, idx.Set(values(types.NewBoolValue(true)), []byte("key")))
	})

	t.Run("Set two values and key succeeds (arity=2)", func(t *testing.T) {
		idx, cleanup := getIndex(t, 2)
		defer cleanup()
		assert.NoError(t, idx.Set(values(types.NewBoolValue(true), types.NewBoolValue(true)), []byte("key")))
	})

	t.Run("Set one value fails (arity=1)", func(t *testing.T) {
		idx, cleanup := getIndex(t, 2)
		defer cleanup()
		assert.Error(t, idx.Set(values(types.NewBoolValue(true)), []byte("key")))
	})

	t.Run("Set two values fails (arity=1)", func(t *testing.T) {
		idx, cleanup := getIndex(t, 1)
		defer cleanup()
		assert.Error(t, idx.Set(values(types.NewBoolValue(true), types.NewBoolValue(true)), []byte("key")))
	})

	t.Run("Set three values fails (arity=2)", func(t *testing.T) {
		idx, cleanup := getIndex(t, 2)
		defer cleanup()
		assert.Error(t, idx.Set(values(types.NewBoolValue(true), types.NewBoolValue(true), types.NewBoolValue(true)), []byte("key")))
	})
}

func TestIndexDelete(t *testing.T) {
	t.Run("Delete valid key succeeds", func(t *testing.T) {
		idx, cleanup := getIndex(t, 1)
		defer cleanup()

		assert.NoError(t, idx.Set(values(types.NewDoubleValue(10)), []byte("key")))
		assert.NoError(t, idx.Set(values(types.NewIntegerValue(10)), []byte("other-key")))
		assert.NoError(t, idx.Set(values(types.NewIntegerValue(11)), []byte("yet-another-key")))
		assert.NoError(t, idx.Set(values(types.NewTextValue("hello")), []byte("yet-another-different-key")))
		assert.NoError(t, idx.Delete(values(types.NewDoubleValue(10)), []byte("key")))

		pivot := values(types.NewIntegerValue(10))
		i := 0
		err := idx.IterateOnRange(&tree.Range{Min: testutil.NewKey(t, pivot...)}, false, func(key tree.Key) error {
			if i == 0 {
				require.Equal(t, "other-key", string(key))
			} else if i == 1 {
				require.Equal(t, "yet-another-key", string(key))
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
		idx, cleanup := getIndex(t, 2)
		defer cleanup()

		assert.NoError(t, idx.Set(values(types.NewDoubleValue(10), types.NewDoubleValue(10)), []byte("key")))
		assert.NoError(t, idx.Set(values(types.NewIntegerValue(10), types.NewIntegerValue(10)), []byte("other-key")))
		assert.NoError(t, idx.Set(values(types.NewIntegerValue(11), types.NewIntegerValue(11)), []byte("yet-another-key")))
		assert.NoError(t, idx.Set(values(types.NewTextValue("hello"), types.NewTextValue("hello")), []byte("yet-another-different-key")))
		assert.NoError(t, idx.Delete(values(types.NewDoubleValue(10), types.NewDoubleValue(10)), []byte("key")))

		pivot := values(types.NewIntegerValue(10), types.NewIntegerValue(10))
		i := 0
		err := idx.IterateOnRange(&tree.Range{Min: testutil.NewKey(t, pivot...)}, false, func(key tree.Key) error {
			if i == 0 {
				require.Equal(t, "other-key", string(key))
			} else if i == 1 {
				require.Equal(t, "yet-another-key", string(key))
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
		idx, cleanup := getIndex(t, 1)
		defer cleanup()

		assert.Error(t, idx.Delete(values(types.NewTextValue("foo")), []byte("foo")))
	})
}

func TestIndexExists(t *testing.T) {
	idx, cleanup := getIndex(t, 2)
	defer cleanup()

	assert.NoError(t, idx.Set(values(types.NewDoubleValue(10), types.NewIntegerValue(11)), []byte("key1")))
	assert.NoError(t, idx.Set(values(types.NewDoubleValue(10), types.NewIntegerValue(12)), []byte("key2")))

	ok, key, err := idx.Exists(values(types.NewDoubleValue(10), types.NewIntegerValue(11)))
	assert.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, tree.Key([]byte("key1")), key)

	ok, _, err = idx.Exists(values(types.NewDoubleValue(11), types.NewIntegerValue(11)))
	assert.NoError(t, err)
	require.False(t, ok)
}

func TestIndexAscendGreaterThan(t *testing.T) {
	t.Run("Should not iterate if index is empty", func(t *testing.T) {
		idx, cleanup := getIndex(t, 1)
		defer cleanup()

		i := 0
		err := idx.IterateOnRange(&tree.Range{Min: testutil.NewKey(t, types.NewIntegerValue(0))}, false, func(key tree.Key) error {
			i++
			return errors.New("should not iterate")
		})
		assert.NoError(t, err)
		require.Equal(t, 0, i)
	})

	t.Run("Should iterate through documents in order, ", func(t *testing.T) {
		noiseBlob := func(i int) []types.Value {
			t.Helper()
			return []types.Value{types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10))}
		}
		noiseInts := func(i int) []types.Value {
			t.Helper()
			return []types.Value{types.NewIntegerValue(int64(i))}
		}

		noCallEq := func(t *testing.T, i uint8, key tree.Key) {
			require.Fail(t, "equality test should not be called here")
		}

		// the following tests will use that constant to determine how many values needs to be inserted
		// with the value and noise generators.
		total := 5

		tests := []struct {
			name string
			// the index type(s) that is being used
			arity int
			// the pivot, typed or not used to iterate
			pivot database.Pivot
			// the generator for the values that are being indexed
			val func(i int) []types.Value
			// the generator for the noise values that are being indexed
			noise func(i int) []types.Value
			// the function to compare the key/value that the iteration yields
			expectedEq func(t *testing.T, i uint8, key tree.Key)
			// the total count of iteration that should happen
			expectedCount int
			mustPanic     bool
		}{
			// integers ---------------------------------------------------
			{name: "vals=integers, pivot=integer",
				arity: 1,
				pivot: values(types.NewIntegerValue(0)),
				val:   func(i int) []types.Value { return values(types.NewIntegerValue(int64(i))) },
				noise: noiseBlob,
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 5,
			},
			{name: "index=integer, vals=integers, pivot=integer",
				arity: 1,
				pivot: values(types.NewIntegerValue(0)),
				val:   func(i int) []types.Value { return values(types.NewIntegerValue(int64(i))) },
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 5,
			},
			{name: "vals=integers, pivot=integer:2",
				arity: 1,
				pivot: values(types.NewIntegerValue(2)),
				val:   func(i int) []types.Value { return values(types.NewIntegerValue(int64(i))) },
				noise: noiseBlob,
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					i += 2
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 3,
			},
			{name: "vals=integers, pivot=integer:10",
				arity:         1,
				pivot:         values(types.NewIntegerValue(10)),
				val:           func(i int) []types.Value { return values(types.NewIntegerValue(int64(i))) },
				noise:         noiseBlob,
				expectedEq:    noCallEq,
				expectedCount: 0,
			},
			{name: "index=integer, vals=integers, pivot=integer:2",
				arity: 1,
				pivot: values(types.NewIntegerValue(2)),
				val:   func(i int) []types.Value { return values(types.NewIntegerValue(int64(i))) },
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					i += 2
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 3,
			},
			{name: "index=integer, vals=integers, pivot=double",
				arity:         1,
				pivot:         values(types.NewDoubleValue(0)),
				val:           func(i int) []types.Value { return values(types.NewIntegerValue(int64(i))) },
				expectedEq:    noCallEq,
				expectedCount: 0,
			},

			// doubles ----------------------------------------------------
			{name: "vals=doubles, pivot=double",
				arity: 1,
				pivot: values(types.NewDoubleValue(0)),
				val:   func(i int) []types.Value { return values(types.NewDoubleValue(float64(i) + float64(i)/2)) },
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 5,
			},
			{name: "vals=doubles, pivot=double:1.8",
				arity: 1,
				pivot: values(types.NewDoubleValue(1.8)),
				val:   func(i int) []types.Value { return values(types.NewDoubleValue(float64(i) + float64(i)/2)) },
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					i += 2
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 3,
			},
			{name: "index=double, vals=doubles, pivot=double:1.8",
				arity: 1,
				pivot: values(types.NewDoubleValue(1.8)),
				val:   func(i int) []types.Value { return values(types.NewDoubleValue(float64(i) + float64(i)/2)) },
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					i += 2
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 3,
			},
			{name: "vals=doubles, pivot=double:10.8",
				arity:         1,
				pivot:         values(types.NewDoubleValue(10.8)),
				val:           func(i int) []types.Value { return values(types.NewDoubleValue(float64(i) + float64(i)/2)) },
				expectedEq:    noCallEq,
				expectedCount: 0,
			},

			// text -------------------------------------------------------
			{name: "vals=text pivot=text",
				arity: 1,
				pivot: values(types.NewTextValue("")),
				val:   func(i int) []types.Value { return values(types.NewTextValue(strconv.Itoa(i))) },
				noise: noiseInts,
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 5,
			},
			{name: "vals=text, pivot=text('2')",
				arity: 1,
				pivot: values(types.NewTextValue("2")),
				val:   func(i int) []types.Value { return values(types.NewTextValue(strconv.Itoa(i))) },
				noise: noiseInts,
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					i += 2
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 3,
			},
			{name: "vals=text, pivot=text('')",
				arity: 1,
				pivot: values(types.NewTextValue("")),
				val:   func(i int) []types.Value { return values(types.NewTextValue(strconv.Itoa(i))) },
				noise: noiseInts,
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 5,
			},
			{name: "vals=text, pivot=text('foo')",
				arity:         1,
				pivot:         values(types.NewTextValue("foo")),
				val:           func(i int) []types.Value { return values(types.NewTextValue(strconv.Itoa(i))) },
				noise:         noiseInts,
				expectedEq:    noCallEq,
				expectedCount: 0,
			},
			{name: "index=text, vals=text, pivot=text('2')",
				arity: 1,
				pivot: values(types.NewTextValue("2")),
				val:   func(i int) []types.Value { return values(types.NewTextValue(strconv.Itoa(i))) },
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					i += 2
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 3,
			},
			// composite --------------------------------------------------
			{name: "vals=[int, int], noise=[blob, blob], pivot=[int, int]",
				arity: 2,
				pivot: values(types.NewIntegerValue(0), types.NewIntegerValue(0)),
				val: func(i int) []types.Value {
					return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
				},
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 5,
			},
			{name: "vals=[int, int], noise=[blob, blob], pivot=[int]",
				arity: 2,
				pivot: values(types.NewIntegerValue(0)),
				val: func(i int) []types.Value {
					return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
				},
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 5,
			},
			{name: "vals=[int, int], noise=[blob, blob], pivot=[0, int, 0]",
				arity: 3,
				pivot: values(types.NewIntegerValue(0), types.NewIntegerValue(0), types.NewIntegerValue(0)),
				val: func(i int) []types.Value {
					return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)), types.NewIntegerValue(int64(i+1)))
				},
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 5,
			},
			{name: "vals=[int, int], noise=[blob, blob], pivot=[int, 0]",
				arity: 2,
				pivot: values(types.NewIntegerValue(0), types.NewIntegerValue(0)),
				val: func(i int) []types.Value {
					return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
				},
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 5,
			},
			{name: "vals=[int, int], noise=[blob, blob], pivot=[0, 0]",
				arity: 2,
				pivot: values(types.NewIntegerValue(0), types.NewIntegerValue(0)),
				val: func(i int) []types.Value {
					return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
				},
				noise: func(i int) []types.Value {
					return values(types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
				},
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 5,
			},
			{name: "vals=[int, int], noise=[blob, blob], pivot=[2, 0]",
				arity: 2,
				pivot: values(types.NewIntegerValue(2), types.NewIntegerValue(0)),
				val: func(i int) []types.Value {
					return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
				},
				noise: func(i int) []types.Value {
					return values(types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
				},
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					i += 2
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 3,
			},
			{name: "vals=[int, int], noise=[blob, blob], pivot=[2, int]",
				arity: 2,
				pivot: values(types.NewIntegerValue(2), types.NewIntegerValue(0)),
				val: func(i int) []types.Value {
					return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
				},
				noise: func(i int) []types.Value {
					return values(types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
				},
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					i += 2
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 3,
			},
			// pivot [2, int] should filter out [2, not(int)]
			{name: "vals=[int, int], noise=[int, blob], pivot=[2, int]",
				arity: 2,
				pivot: values(types.NewIntegerValue(2), types.NewIntegerValue(0)),
				val: func(i int) []types.Value {
					return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
				},
				noise: func(i int) []types.Value {
					// only [3, not(int)] is greater than [2, int], so it will appear anyway if we don't skip it
					if i < 3 {
						return values(types.NewIntegerValue(int64(i)), types.NewBoolValue(true))
					}

					return nil
				},
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					i += 2
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 3,
			},
			// a more subtle case
			{name: "vals=[int, blob], noise=[blob, blob], pivot=[2, 'a']", // pivot is [2, a] but value is [2, c] but that must work anyway
				arity: 2,
				pivot: values(types.NewIntegerValue(2), types.NewBlobValue([]byte{byte('a')})),
				val: func(i int) []types.Value {
					return values(types.NewIntegerValue(int64(i)), types.NewBlobValue([]byte{byte('a' + uint8(i))}))
				},
				noise: func(i int) []types.Value {
					return values(types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
				},
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					i += 2
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 3,
			},
			// partial pivot
			{name: "vals=[int, int], noise=[blob, blob], pivot=[0]",
				arity: 2,
				pivot: values(types.NewIntegerValue(0)),
				val: func(i int) []types.Value {
					return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
				},
				noise: func(i int) []types.Value {
					return values(types.NewIntegerValue(int64(i)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
				},
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					// let's not try to match, it's not important
				},
				expectedCount: 10,
			},
			{name: "vals=[int, int], noise=[blob, blob], pivot=[2]",
				arity: 2,
				pivot: values(types.NewIntegerValue(2)),
				val: func(i int) []types.Value {
					return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
				},
				noise: func(i int) []types.Value {
					return values(types.NewIntegerValue(int64(i)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
				},
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					// let's not try to match, it's not important
				},
				expectedCount: 6, // total * 2 - (noise + val = 2) * 2
			},
			// this is a tricky test, when we have multiple values but they share the first pivot element;
			// this is by definition a very implementation dependent test.
			{name: "vals=[int, int], noise=int, bool], pivot=[int:0, int:0]",
				arity: 2,
				pivot: values(types.NewIntegerValue(0), types.NewIntegerValue(0)),
				val: func(i int) []types.Value {
					return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
				},
				noise: func(i int) []types.Value {
					return values(types.NewIntegerValue(int64(i)), types.NewBoolValue(true))
				},
				// [0, 0] > [0, true] but [1, true] > [0, 0] so we will see some bools in the results
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					if i%2 == 0 {
						i = i / 2
						require.Equal(t, []byte{'a' + i}, []byte(key))
					}
				},
				expectedCount: 9, // 10 elements, but pivot skipped the initial [0, true]
			},
			// index typed
			{name: "index=[int, int], vals=[int, int], pivot=[0, 0]",
				arity: 2,
				pivot: values(types.NewIntegerValue(0), types.NewIntegerValue(0)),
				val: func(i int) []types.Value {
					return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
				},
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 5,
			},
			{name: "index=[int, int], vals=[int, int], pivot=[2, 0]",
				arity: 2,
				pivot: values(types.NewIntegerValue(2), types.NewIntegerValue(0)),
				val: func(i int) []types.Value {
					return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
				},
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					i += 2
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 3,
			},
			// a more subtle case
			{name: "vals=[int, blob], pivot=[2, 'a']", // pivot is [2, a] but value is [2, c] but that must work anyway
				arity: 2,
				pivot: values(types.NewIntegerValue(2), types.NewBlobValue([]byte{byte('a')})),
				val: func(i int) []types.Value {
					return values(types.NewIntegerValue(int64(i)), types.NewBlobValue([]byte{byte('a' + uint8(i))}))
				},
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					i += 2
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 3,
			},
			// partial pivot
			{name: "vals=[int, int], pivot=[0]",
				arity: 2,
				pivot: values(types.NewIntegerValue(0)),
				val: func(i int) []types.Value {
					return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
				},
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 5,
			},
			{name: "vals=[int, int], pivot=[2]",
				arity: 2,
				pivot: values(types.NewIntegerValue(2)),
				val: func(i int) []types.Value {
					return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
				},
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					i += 2
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 3,
			},

			// documents --------------------------------------------------
			{name: "vals=[doc, int], pivot=[{a:2}, 3]",
				arity: 2,
				pivot: values(
					types.NewDocumentValue(testutil.MakeDocument(t, `{"a":2}`)),
					types.NewIntegerValue(int64(3)),
				),
				val: func(i int) []types.Value {
					return values(
						types.NewDocumentValue(testutil.MakeDocument(t, `{"a":`+strconv.Itoa(i)+`}`)),
						types.NewIntegerValue(int64(i+1)),
					)
				},
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					i += 2
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 3,
			},

			// arrays -----------------------------------------------------
			{name: "vals=[int[], int], pivot=[[2,2], 3]",
				arity: 2,
				pivot: values(
					testutil.MakeArrayValue(t, 2, 2),
					types.NewIntegerValue(int64(3)),
				),
				val: func(i int) []types.Value {
					return values(
						testutil.MakeArrayValue(t, i, i),
						types.NewIntegerValue(int64(i+1)),
					)
				},
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					i += 2
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 3,
			},
			{name: "vals=[int[], int[]], pivot=[[2,2], [3,3]]",
				arity: 2,
				pivot: values(
					testutil.MakeArrayValue(t, 2, 2),
					testutil.MakeArrayValue(t, 3, 3),
				),
				val: func(i int) []types.Value {
					return values(
						testutil.MakeArrayValue(t, i, i),
						testutil.MakeArrayValue(t, i+1, i+1),
					)
				},
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					i += 2
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 3,
			},
			{name: "vals=[int[], int], pivot=[[2,2], 3]",
				arity: 2,
				pivot: values(
					testutil.MakeArrayValue(t, 2, 2),
					types.NewIntegerValue(int64(3)),
				),
				val: func(i int) []types.Value {
					return values(
						testutil.MakeArrayValue(t, i, i),
						types.NewIntegerValue(int64(i+1)),
					)
				},
				expectedEq: func(t *testing.T, i uint8, key tree.Key) {
					i += 2
					require.Equal(t, []byte{'a' + i}, []byte(key))
				},
				expectedCount: 3,
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				idx, cleanup := getIndex(t, test.arity)
				defer cleanup()

				for i := 0; i < total; i++ {
					assert.NoError(t, idx.Set(test.val(i), []byte{'a' + byte(i)}))
					if test.noise != nil {
						v := test.noise(i)
						if v != nil {
							assert.NoError(t, idx.Set(test.noise(i), []byte{'a' + byte(i)}))
						}
					}
				}

				var i uint8
				var count int
				fn := func() error {
					return idx.IterateOnRange(&tree.Range{Min: testutil.NewKey(t, test.pivot...)}, false, func(key tree.Key) error {
						test.expectedEq(t, i, key)
						i++
						count++
						return nil
					})
				}
				if test.mustPanic {
					// let's avoid panicking because expectedEq wasn't defined, which would
					// be a false positive.
					if test.expectedEq == nil {
						test.expectedEq = func(t *testing.T, i uint8, key tree.Key) {}
					}
					require.Panics(t, func() { _ = fn() })
				} else {
					err := fn()
					assert.NoError(t, err)
					require.Equal(t, test.expectedCount, count)
				}
			})
		}
	})
}

func TestIndexDescendLessOrEqual(t *testing.T) {
	for _, unique := range []bool{true, false} {
		text := fmt.Sprintf("Unique: %v, ", unique)

		t.Run(text+"Should iterate through documents in order, ", func(t *testing.T) {
			noiseBlob := func(i int) []types.Value {
				t.Helper()
				return []types.Value{types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10))}
			}
			noiseInts := func(i int) []types.Value {
				t.Helper()
				return []types.Value{types.NewIntegerValue(int64(i))}
			}

			noCallEq := func(t *testing.T, i uint8, key tree.Key) {
				require.Fail(t, "equality test should not be called here")
			}

			// the following tests will use that constant to determine how many values needs to be inserted
			// with the value and noise generators.
			total := 5

			tests := []struct {
				name string
				// the index type(s) that is being used
				arity int
				// the pivot, typed or not used to iterate
				pivot database.Pivot
				// the generator for the values that are being indexed
				val func(i int) []types.Value
				// the generator for the noise values that are being indexed
				noise func(i int) []types.Value
				// the function to compare the key/value that the iteration yields
				expectedEq func(t *testing.T, i uint8, key tree.Key)
				// the total count of iteration that should happen
				expectedCount int
				mustPanic     bool
			}{
				// integers ---------------------------------------------------
				{name: "vals=integers, pivot=integer",
					arity: 1,
					pivot: values(types.NewIntegerValue(5)),
					val:   func(i int) []types.Value { return values(types.NewIntegerValue(int64(i))) },
					noise: noiseBlob,
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						require.Equal(t, tree.Key([]byte{'a' + i}), key)
					},
					expectedCount: 5,
				},
				{name: "vals=integers, pivot=integer:2",
					arity: 1,
					pivot: values(types.NewIntegerValue(2)),
					val:   func(i int) []types.Value { return values(types.NewIntegerValue(int64(i))) },
					noise: noiseBlob,
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						i -= 2
						require.Equal(t, []byte{'a' + i}, []byte(key))
					},
					expectedCount: 3,
				},
				{name: "vals=integers, pivot=integer:-10",
					arity:         1,
					pivot:         values(types.NewIntegerValue(-10)),
					val:           func(i int) []types.Value { return values(types.NewIntegerValue(int64(i))) },
					noise:         noiseBlob,
					expectedEq:    noCallEq,
					expectedCount: 0,
				},
				{name: "vals=integers, pivot=double",
					arity:         1,
					pivot:         values(types.NewDoubleValue(0)),
					val:           func(i int) []types.Value { return values(types.NewIntegerValue(int64(i))) },
					noise:         noiseBlob,
					expectedEq:    noCallEq,
					expectedCount: 0,
				},

				// doubles ----------------------------------------------------
				{name: "vals=doubles, pivot=double",
					arity: 1,
					pivot: values(types.NewDoubleValue(10)),
					val:   func(i int) []types.Value { return values(types.NewDoubleValue(float64(i) + float64(i)/2)) },
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						require.Equal(t, []byte{'a' + i}, []byte(key))
					},
					expectedCount: 5,
				},
				{name: "vals=doubles, pivot=double:1.8",
					arity: 1,
					pivot: values(types.NewDoubleValue(1.8)),
					val:   func(i int) []types.Value { return values(types.NewDoubleValue(float64(i) + float64(i)/2)) },
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						i -= 3
						require.Equal(t, []byte{'a' + i}, []byte(key))
					},
					expectedCount: 2,
				},
				{name: "vals=doubles, pivot=double:-10.8",
					arity:         1,
					pivot:         values(types.NewDoubleValue(-10.8)),
					val:           func(i int) []types.Value { return values(types.NewDoubleValue(float64(i) + float64(i)/2)) },
					expectedEq:    noCallEq,
					expectedCount: 0,
				},

				// text -------------------------------------------------------
				{name: "vals=text pivot=text",
					arity: 1,
					pivot: values(types.NewTextValue("7")),
					val:   func(i int) []types.Value { return values(types.NewTextValue(strconv.Itoa(i))) },
					noise: noiseInts,
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						require.Equal(t, []byte{'a' + i}, []byte(key))
					},
					expectedCount: 5,
				},
				{name: "vals=text, pivot=text('2')",
					arity: 1,
					pivot: values(types.NewTextValue("2")),
					val:   func(i int) []types.Value { return values(types.NewTextValue(strconv.Itoa(i))) },
					noise: noiseInts,
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						i -= 2
						require.Equal(t, []byte{'a' + i}, []byte(key))
					},
					expectedCount: 3,
				},
				{name: "vals=text, pivot=text('')",
					arity:         1,
					pivot:         values(types.NewTextValue("")),
					val:           func(i int) []types.Value { return values(types.NewTextValue(strconv.Itoa(i))) },
					noise:         noiseInts,
					expectedEq:    noCallEq,
					expectedCount: 0,
				},
				{name: "vals=text, pivot=text('foo')",
					arity: 1,
					pivot: values(types.NewTextValue("foo")),
					val:   func(i int) []types.Value { return values(types.NewTextValue(strconv.Itoa(i))) },
					noise: noiseInts,
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						require.Equal(t, []byte{'a' + i}, []byte(key))
					},
					expectedCount: 5,
				},

				// composite --------------------------------------------------
				{name: "vals=[int, int], noise=[blob, blob], pivot=[int]",
					arity: 2,
					pivot: values(types.NewIntegerValue(7)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						require.Equal(t, []byte{'a' + i}, []byte(key))
					},
					expectedCount: 5,
				},
				{name: "vals=[int, int], noise=[blob, blob], pivot=[0, 0]",
					arity: 2,
					pivot: values(types.NewIntegerValue(0), types.NewIntegerValue(0)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []types.Value {
						return values(types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq:    noCallEq,
					expectedCount: 0,
				},
				{name: "vals=[int, int], noise=[blob, blob], pivot=[5, 5]",
					arity: 2,
					pivot: values(types.NewIntegerValue(5), types.NewIntegerValue(5)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []types.Value {
						return values(types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						require.Equal(t, []byte{'a' + i}, []byte(key))
					},
					expectedCount: 5,
				},
				// [0,1], [1,2], --[2,0]--,  [2,3], [3,4], [4,5]
				{name: "vals=[int, int], noise=[blob, blob], pivot=[2, 0]",
					arity: 2,
					pivot: values(types.NewIntegerValue(2), types.NewIntegerValue(0)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []types.Value {
						return values(types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						i -= 3
						require.Equal(t, []byte{'a' + i}, []byte(key))
					},
					expectedCount: 2,
				},
				// [0,1], [1,2], [2,3], --[2,3]--, [3,4], [4,5]
				{name: "vals=[int, int], noise=[blob, blob], pivot=[2, 3]",
					arity: 2,
					pivot: values(types.NewIntegerValue(2), types.NewIntegerValue(3)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []types.Value {
						return values(types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						i -= 2
						require.Equal(t, []byte{'a' + i}, []byte(key))
					},
					expectedCount: 3,
				},
				// pivot [2, int] should filter out [2, not(int)]
				// [0,1], [1,2], [2,3], --[2,int]--, [2, text], [3,4], [3,text], [4,5], [4,text]
				{name: "vals=[int, int], noise=[int, text], pivot=[2, 3]",
					arity: 2,
					pivot: values(types.NewIntegerValue(2), types.NewIntegerValue(3)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []types.Value {
						if i > 1 {
							return values(types.NewIntegerValue(int64(i)), types.NewTextValue("foo"))
						}

						return nil
					},
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						i -= 2
						require.Equal(t, []byte{'a' + i}, []byte(key))
					},
					expectedCount: 3,
				},
				// a more subtle case
				{name: "vals=[int, blob], noise=[blob, blob], pivot=[2, 'a']", // pivot is [2, a] but value is [2, c] but that must work anyway
					arity: 2,
					pivot: values(types.NewIntegerValue(2), types.NewBlobValue([]byte{byte('a')})),
					val: func(i int) []types.Value {
						return values(
							types.NewIntegerValue(int64(i)),
							types.NewBlobValue([]byte{byte('a' + uint8(i))}),
						)
					},
					noise: func(i int) []types.Value {
						return values(
							types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)),
							types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)),
						)
					},
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						i -= 3
						require.Equal(t, []byte{'a' + i}, []byte(key))
					},
					expectedCount: 2,
				},
				// partial pivot
				{name: "vals=[int, int], noise=[blob, blob], pivot=[0]",
					arity: 2,
					pivot: values(types.NewIntegerValue(0)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						// let's not try to match, it's not important
					},
					expectedCount: 2, // [0] is "equal" to [0, 1] and [0, "1"]
				},
				{name: "vals=[int, int], noise=[blob, blob], pivot=[5]",
					arity: 2,
					pivot: values(types.NewIntegerValue(5)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						// let's not try to match, it's not important
					},
					expectedCount: 10,
				},
				{name: "vals=[int, int], noise=[blob, blob], pivot=[2]",
					arity: 2,
					pivot: values(types.NewIntegerValue(2)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						// let's not try to match, it's not important
					},
					expectedCount: 6, // total * 2 - (noise + val = 2) * 2
				},
				{name: "vals=[int, int], pivot=[0, 0]",
					arity: 2,
					pivot: values(types.NewIntegerValue(0), types.NewIntegerValue(0)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					expectedEq:    noCallEq,
					expectedCount: 0,
				},
				{name: "vals=[int, int], pivot=[5, 6]",
					arity: 2,
					pivot: values(types.NewIntegerValue(5), types.NewIntegerValue(6)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						require.Equal(t, []byte{'a' + i}, []byte(key))
					},
					expectedCount: 5,
				},
				{name: "vals=[int, int], pivot=[2, 0]",
					arity: 2,
					pivot: values(types.NewIntegerValue(2), types.NewIntegerValue(0)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						i -= 3
						require.Equal(t, []byte{'a' + i}, []byte(key))
					},
					expectedCount: 2,
				},
				// a more subtle case
				{name: "vals=[int, blob], pivot=[2, 'a']", // pivot is [2, a] but value is [2, c] but that must work anyway
					arity: 2,
					pivot: values(types.NewIntegerValue(2), types.NewBlobValue([]byte{byte('a')})),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewBlobValue([]byte{byte('a' + uint8(i))}))
					},
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						i -= 3
						require.Equal(t, []byte{'a' + i}, []byte(key))
					},
					expectedCount: 2,
				},
				// partial pivot
				{name: "vals=[int, int], pivot=[0]",
					arity: 2,
					pivot: values(types.NewIntegerValue(0)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						i -= 4
						require.Equal(t, []byte{'a' + i}, []byte(key))
					},
					expectedCount: 1,
				},
				// [0,1], [1,2], [2,3], --[2]--, [3,4], [4,5]
				{name: "index=[int, int], vals=[int, int], pivot=[2]",
					arity: 2,
					pivot: values(types.NewIntegerValue(2)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						i -= 2
						require.Equal(t, []byte{'a' + i}, []byte(key))
					},
					expectedCount: 3,
				},
				// documents --------------------------------------------------
				{name: "vals=[doc, int], pivot=[{a:2}, 3]",
					arity: 2,
					pivot: values(
						types.NewDocumentValue(testutil.MakeDocument(t, `{"a":2}`)),
						types.NewIntegerValue(int64(3)),
					),
					val: func(i int) []types.Value {
						return values(
							types.NewDocumentValue(testutil.MakeDocument(t, `{"a":`+strconv.Itoa(i)+`}`)),
							types.NewIntegerValue(int64(i+1)),
						)
					},
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						i -= 2
						require.Equal(t, []byte{'a' + i}, []byte(key))
					},
					expectedCount: 3,
				},
				{name: "index=[document, int], vals=[doc, int], pivot=[{a:2}, 3]",
					arity: 2,
					pivot: values(
						types.NewDocumentValue(testutil.MakeDocument(t, `{"a":2}`)),
						types.NewIntegerValue(int64(3)),
					),
					val: func(i int) []types.Value {
						return values(
							types.NewDocumentValue(testutil.MakeDocument(t, `{"a":`+strconv.Itoa(i)+`}`)),
							types.NewIntegerValue(int64(i+1)),
						)
					},
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						i -= 2
						require.Equal(t, []byte{'a' + i}, []byte(key))
					},
					expectedCount: 3,
				},

				// arrays -----------------------------------------------------
				{name: "vals=[int[], int], pivot=[[2,2], 3]",
					arity: 2,
					pivot: values(
						testutil.MakeArrayValue(t, 2, 2),
						types.NewIntegerValue(int64(3)),
					),
					val: func(i int) []types.Value {
						return values(
							testutil.MakeArrayValue(t, i, i),
							types.NewIntegerValue(int64(i+1)),
						)
					},
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						i -= 2
						require.Equal(t, []byte{'a' + i}, []byte(key))
					},
					expectedCount: 3,
				},
				{name: "vals=[int[], int[]], pivot=[[2,2], [3,3]]",
					arity: 2,
					pivot: values(
						testutil.MakeArrayValue(t, 2, 2),
						testutil.MakeArrayValue(t, 3, 3),
					),
					val: func(i int) []types.Value {
						return values(
							testutil.MakeArrayValue(t, i, i),
							testutil.MakeArrayValue(t, i+1, i+1),
						)
					},
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						i -= 2
						require.Equal(t, []byte{'a' + i}, []byte(key))
					},
					expectedCount: 3,
				},
				{name: "index=[array, any], vals=[int[], int], pivot=[[2,2], 3]",
					arity: 2,
					pivot: values(
						testutil.MakeArrayValue(t, 2, 2),
						types.NewIntegerValue(int64(3)),
					),
					val: func(i int) []types.Value {
						return values(
							testutil.MakeArrayValue(t, i, i),
							types.NewIntegerValue(int64(i+1)),
						)
					},
					expectedEq: func(t *testing.T, i uint8, key tree.Key) {
						i -= 2
						require.Equal(t, []byte{'a' + i}, []byte(key))
					},
					expectedCount: 3,
				},
			}

			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					idx, cleanup := getIndex(t, test.arity)
					defer cleanup()

					for i := 0; i < total; i++ {
						assert.NoError(t, idx.Set(test.val(i), []byte{'a' + byte(i)}))
						if test.noise != nil {
							v := test.noise(i)
							if v != nil {
								assert.NoError(t, idx.Set(test.noise(i), []byte{'n', 'a' + byte(i)}))
							}
						}
					}

					var i uint8
					var count int

					fn := func() error {
						t.Helper()
						return idx.IterateOnRange(&tree.Range{Max: testutil.NewKey(t, test.pivot...)}, true, func(key tree.Key) error {
							test.expectedEq(t, uint8(total-1)-i, key)
							i++
							count++
							return nil
						})
					}
					if test.mustPanic {
						// let's avoid panicking because expectedEq wasn't defined, which would
						// be a false positive.
						if test.expectedEq == nil {
							test.expectedEq = func(t *testing.T, i uint8, key tree.Key) {}
						}
						require.Panics(t, func() {
							_ = fn()
						})
					} else {
						err := fn()
						assert.NoError(t, err)
						require.Equal(t, test.expectedCount, count)
					}
				})
			}
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
				idx, cleanup := getIndex(b, 1)

				b.StartTimer()
				for j := 0; j < size; j++ {
					k := fmt.Sprintf("name-%d", j)
					idx.Set(values(types.NewTextValue(k)), []byte(k))
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
			idx, cleanup := getIndex(b, 1)
			defer cleanup()

			for i := 0; i < size; i++ {
				k := []byte(fmt.Sprintf("name-%d", i))
				_ = idx.Set(values(types.NewTextValue(string(k))), k)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = idx.IterateOnRange(&tree.Range{Min: testutil.NewKey(b, types.NewTextValue(""))}, false, func(_ tree.Key) error {
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
				idx, cleanup := getIndex(b, 2)

				b.StartTimer()
				for j := 0; j < size; j++ {
					k := fmt.Sprintf("name-%d", j)
					idx.Set(values(types.NewTextValue(k), types.NewTextValue(k)), []byte(k))
				}
				b.StopTimer()
				cleanup()
			}
		})
	}
}

// BenchmarkCompositeIndexIteration benchmarks the iterarion of a cursor with 1, 10, 1000 and 10000 items.
func BenchmarkCompositeIndexIteration(b *testing.B) {
	for size := 10; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			idx, cleanup := getIndex(b, 2)
			defer cleanup()

			for i := 0; i < size; i++ {
				k := []byte(fmt.Sprintf("name-%d", i))
				_ = idx.Set(values(types.NewTextValue(string(k)), types.NewTextValue(string(k))), k)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = idx.IterateOnRange(&tree.Range{Min: testutil.NewKey(b, types.NewTextValue(""), types.NewTextValue(""))}, false, func(_ tree.Key) error {
					return nil
				})
			}
			b.StopTimer()
		})
	}
}
