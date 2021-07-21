package database_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/engine/memoryengine"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

// values is a helper function to avoid having to type []types.Value{} all the time.
func values(vs ...types.Value) []types.Value {
	return vs
}

func getIndex(t testing.TB, unique bool, types ...types.ValueType) (*database.Index, func()) {
	ng := memoryengine.NewEngine()
	tx, err := ng.Begin(context.Background(), engine.TxOptions{
		Writable: true,
	})
	require.NoError(t, err)

	idx := database.NewIndex(tx, "foo", &database.IndexInfo{Unique: unique, Types: types})

	return idx, func() {
		tx.Rollback()
	}
}

func TestIndexSet(t *testing.T) {
	for _, unique := range []bool{true, false} {
		text := fmt.Sprintf("Unique: %v, ", unique)

		t.Run(text+"Set nil key falls (arity=1)", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique)
			defer cleanup()
			require.Error(t, idx.Set(values(types.NewBoolValue(true)), nil))
		})

		t.Run(text+"Set value and key succeeds (arity=1)", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique)
			defer cleanup()
			require.NoError(t, idx.Set(values(types.NewBoolValue(true)), []byte("key")))
		})

		t.Run(text+"Set two values and key succeeds (arity=2)", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique, types.AnyType, types.AnyType)
			defer cleanup()
			require.NoError(t, idx.Set(values(types.NewBoolValue(true), types.NewBoolValue(true)), []byte("key")))
		})

		t.Run(text+"Set one value fails (arity=1)", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique, types.AnyType, types.AnyType)
			defer cleanup()
			require.Error(t, idx.Set(values(types.NewBoolValue(true)), []byte("key")))
		})

		t.Run(text+"Set two values fails (arity=1)", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique, types.AnyType)
			defer cleanup()
			require.Error(t, idx.Set(values(types.NewBoolValue(true), types.NewBoolValue(true)), []byte("key")))
		})

		t.Run(text+"Set three values fails (arity=2)", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique, types.AnyType, types.AnyType)
			defer cleanup()
			require.Error(t, idx.Set(values(types.NewBoolValue(true), types.NewBoolValue(true), types.NewBoolValue(true)), []byte("key")))
		})
	}

	t.Run("Unique: true, Duplicate", func(t *testing.T) {
		idx, cleanup := getIndex(t, true)
		defer cleanup()

		require.NoError(t, idx.Set(values(types.NewIntegerValue(10)), []byte("key")))
		require.NoError(t, idx.Set(values(types.NewIntegerValue(11)), []byte("key")))
		require.Equal(t, database.ErrIndexDuplicateValue, idx.Set(values(types.NewIntegerValue(10)), []byte("key")))
	})

	t.Run("Unique: true, Type: integer Duplicate", func(t *testing.T) {
		idx, cleanup := getIndex(t, true, types.IntegerValue)
		defer cleanup()

		require.NoError(t, idx.Set(values(types.NewIntegerValue(10)), []byte("key")))
		require.NoError(t, idx.Set(values(types.NewIntegerValue(11)), []byte("key")))
		require.Equal(t, database.ErrIndexDuplicateValue, idx.Set(values(types.NewIntegerValue(10)), []byte("key")))
	})

	t.Run("Unique: true, Type: (integer, integer) Duplicate,", func(t *testing.T) {
		idx, cleanup := getIndex(t, true, types.IntegerValue, types.IntegerValue)
		defer cleanup()

		require.NoError(t, idx.Set(values(types.NewIntegerValue(10), types.NewIntegerValue(10)), []byte("key")))
		require.NoError(t, idx.Set(values(types.NewIntegerValue(10), types.NewIntegerValue(11)), []byte("key")))
		require.NoError(t, idx.Set(values(types.NewIntegerValue(11), types.NewIntegerValue(11)), []byte("key")))
		require.Equal(t, database.ErrIndexDuplicateValue, idx.Set(values(types.NewIntegerValue(10), types.NewIntegerValue(10)), []byte("key")))
	})

	t.Run("Unique: true, Type: (integer, text) Duplicate,", func(t *testing.T) {
		idx, cleanup := getIndex(t, true, types.IntegerValue, types.TextValue)
		defer cleanup()

		require.NoError(t, idx.Set(values(types.NewIntegerValue(10), types.NewTextValue("foo")), []byte("key")))
		require.NoError(t, idx.Set(values(types.NewIntegerValue(11), types.NewTextValue("foo")), []byte("key")))
		require.Equal(t, database.ErrIndexDuplicateValue, idx.Set(values(types.NewIntegerValue(10), types.NewTextValue("foo")), []byte("key")))
	})
}

func TestIndexDelete(t *testing.T) {
	t.Run("Unique: false, Delete valid key succeeds", func(t *testing.T) {
		idx, cleanup := getIndex(t, false)
		defer cleanup()

		require.NoError(t, idx.Set(values(types.NewDoubleValue(10)), []byte("key")))
		require.NoError(t, idx.Set(values(types.NewIntegerValue(10)), []byte("other-key")))
		require.NoError(t, idx.Set(values(types.NewIntegerValue(11)), []byte("yet-another-key")))
		require.NoError(t, idx.Set(values(types.NewTextValue("hello")), []byte("yet-another-different-key")))
		require.NoError(t, idx.Delete(values(types.NewDoubleValue(10)), []byte("key")))

		pivot := values(types.NewIntegerValue(10))
		i := 0
		err := idx.AscendGreaterOrEqual(pivot, func(v, k []byte) error {
			if i == 0 {
				requireEqualBinary(t, testutil.MakeArrayValue(t, 10), v)
				require.Equal(t, "other-key", string(k))
			} else if i == 1 {
				requireEqualBinary(t, testutil.MakeArrayValue(t, 11), v)
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

	t.Run("Unique: false, Delete valid key succeeds (arity=2)", func(t *testing.T) {
		idx, cleanup := getIndex(t, false, types.AnyType, types.AnyType)
		defer cleanup()

		require.NoError(t, idx.Set(values(types.NewDoubleValue(10), types.NewDoubleValue(10)), []byte("key")))
		require.NoError(t, idx.Set(values(types.NewIntegerValue(10), types.NewIntegerValue(10)), []byte("other-key")))
		require.NoError(t, idx.Set(values(types.NewIntegerValue(11), types.NewIntegerValue(11)), []byte("yet-another-key")))
		require.NoError(t, idx.Set(values(types.NewTextValue("hello"), types.NewTextValue("hello")), []byte("yet-another-different-key")))
		require.NoError(t, idx.Delete(values(types.NewDoubleValue(10), types.NewDoubleValue(10)), []byte("key")))

		pivot := values(types.NewIntegerValue(10), types.NewIntegerValue(10))
		i := 0
		err := idx.AscendGreaterOrEqual(pivot, func(v, k []byte) error {
			if i == 0 {
				expected := types.NewArrayValue(document.NewValueBuffer(
					types.NewIntegerValue(10),
					types.NewIntegerValue(10),
				))
				requireEqualBinary(t, expected, v)
				require.Equal(t, "other-key", string(k))
			} else if i == 1 {
				expected := types.NewArrayValue(document.NewValueBuffer(
					types.NewIntegerValue(11),
					types.NewIntegerValue(11),
				))
				requireEqualBinary(t, expected, v)
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

		require.NoError(t, idx.Set(values(types.NewIntegerValue(10)), []byte("key1")))
		require.NoError(t, idx.Set(values(types.NewDoubleValue(11)), []byte("key2")))
		require.NoError(t, idx.Set(values(types.NewIntegerValue(12)), []byte("key3")))
		require.NoError(t, idx.Delete(values(types.NewDoubleValue(11)), []byte("key2")))

		i := 0
		err := idx.AscendGreaterOrEqual(values(types.NewEmptyValue(types.IntegerValue)), func(v, k []byte) error {
			switch i {
			case 0:
				requireEqualBinary(t, testutil.MakeArrayValue(t, 10), v)
				require.Equal(t, "key1", string(k))
			case 1:
				requireEqualBinary(t, testutil.MakeArrayValue(t, 12), v)
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

	t.Run("Unique: true, Delete valid key succeeds (arity=2)", func(t *testing.T) {
		idx, cleanup := getIndex(t, true, types.AnyType, types.AnyType)
		defer cleanup()

		require.NoError(t, idx.Set(values(types.NewIntegerValue(10), types.NewIntegerValue(10)), []byte("key1")))
		require.NoError(t, idx.Set(values(types.NewDoubleValue(11), types.NewDoubleValue(11)), []byte("key2")))
		require.NoError(t, idx.Set(values(types.NewIntegerValue(12), types.NewIntegerValue(12)), []byte("key3")))
		require.NoError(t, idx.Delete(values(types.NewDoubleValue(11), types.NewDoubleValue(11)), []byte("key2")))

		i := 0
		// this will break until the [v, int] case is supported
		// pivot := values(types.NewIntegerValue(0), types.NewEmptyValue(types.IntegerValue))
		pivot := values(types.NewIntegerValue(0), types.NewIntegerValue(0))
		err := idx.AscendGreaterOrEqual(pivot, func(v, k []byte) error {
			switch i {
			case 0:
				expected := types.NewArrayValue(document.NewValueBuffer(
					types.NewIntegerValue(10),
					types.NewIntegerValue(10),
				))
				requireEqualBinary(t, expected, v)
				require.Equal(t, "key1", string(k))
			case 1:
				expected := types.NewArrayValue(document.NewValueBuffer(
					types.NewIntegerValue(12),
					types.NewIntegerValue(12),
				))
				requireEqualBinary(t, expected, v)
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

			require.Error(t, idx.Delete(values(types.NewTextValue("foo")), []byte("foo")))
		})
	}
}

func TestIndexExists(t *testing.T) {
	idx, cleanup := getIndex(t, false, types.DoubleValue, types.IntegerValue)
	defer cleanup()

	require.NoError(t, idx.Set(values(types.NewDoubleValue(10), types.NewIntegerValue(11)), []byte("key1")))
	require.NoError(t, idx.Set(values(types.NewDoubleValue(10), types.NewIntegerValue(12)), []byte("key2")))

	ok, key, err := idx.Exists(values(types.NewDoubleValue(10), types.NewIntegerValue(11)))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("key1"), key)

	ok, _, err = idx.Exists(values(types.NewDoubleValue(11), types.NewIntegerValue(11)))
	require.NoError(t, err)
	require.False(t, ok)
}

// requireEqualBinary asserts equality assuming that the value is encoded through marshal binary
func requireEqualBinary(t *testing.T, expected types.Value, actual []byte) {
	t.Helper()

	var buf bytes.Buffer
	err := types.NewValueEncoder(&buf).Encode(expected)
	require.NoError(t, err)

	data := buf.Bytes()
	require.Equal(t, data, actual)
}

func requireIdxEncodedEq(t *testing.T, vs ...types.Value) func([]byte) {
	t.Helper()

	var buf bytes.Buffer
	err := types.NewValueEncoder(&buf).Encode(types.NewArrayValue(document.NewValueBuffer(vs...)))
	require.NoError(t, err)

	return func(actual []byte) {
		t.Helper()

		require.Equal(t, buf.Bytes(), actual)
	}
}

func TestIndexAscendGreaterThan(t *testing.T) {
	for _, unique := range []bool{true, false} {
		text := fmt.Sprintf("Unique: %v, ", unique)

		t.Run(text+"Should not iterate if index is empty", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique)
			defer cleanup()

			i := 0
			err := idx.AscendGreaterOrEqual(values(types.NewEmptyValue(types.IntegerValue)), func(val, key []byte) error {
				i++
				return errors.New("should not iterate")
			})
			require.NoError(t, err)
			require.Equal(t, 0, i)
		})

		t.Run(text+"Should iterate through documents in order, ", func(t *testing.T) {
			noiseBlob := func(i int) []types.Value {
				t.Helper()
				return []types.Value{types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10))}
			}
			noiseInts := func(i int) []types.Value {
				t.Helper()
				return []types.Value{types.NewIntegerValue(int64(i))}
			}

			noCallEq := func(t *testing.T, i uint8, key []byte, val []byte) {
				require.Fail(t, "equality test should not be called here")
			}

			// the following tests will use that constant to determine how many values needs to be inserted
			// with the value and noise generators.
			total := 5

			tests := []struct {
				name string
				// the index type(s) that is being used
				indexTypes []types.ValueType
				// the pivot, typed or not used to iterate
				pivot database.Pivot
				// the generator for the values that are being indexed
				val func(i int) []types.Value
				// the generator for the noise values that are being indexed
				noise func(i int) []types.Value
				// the function to compare the key/value that the iteration yields
				expectedEq func(t *testing.T, i uint8, key []byte, val []byte)
				// the total count of iteration that should happen
				expectedCount int
				mustPanic     bool
			}{
				// integers ---------------------------------------------------
				{name: "index=any, vals=integers, pivot=integer",
					indexTypes: nil,
					pivot:      values(types.NewEmptyValue(types.IntegerValue)),
					val:        func(i int) []types.Value { return values(types.NewIntegerValue(int64(i))) },
					noise:      noiseBlob,
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=integer, vals=integers, pivot=integer",
					indexTypes: []types.ValueType{types.IntegerValue},
					pivot:      values(types.NewEmptyValue(types.IntegerValue)),
					val:        func(i int) []types.Value { return values(types.NewIntegerValue(int64(i))) },
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=any, vals=integers, pivot=integer:2",
					indexTypes: nil,
					pivot:      values(types.NewIntegerValue(2)),
					val:        func(i int) []types.Value { return values(types.NewIntegerValue(int64(i))) },
					noise:      noiseBlob,
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=any, vals=integers, pivot=integer:10",
					indexTypes:    nil,
					pivot:         values(types.NewIntegerValue(10)),
					val:           func(i int) []types.Value { return values(types.NewIntegerValue(int64(i))) },
					noise:         noiseBlob,
					expectedEq:    noCallEq,
					expectedCount: 0,
				},
				{name: "index=integer, vals=integers, pivot=integer:2",
					indexTypes: []types.ValueType{types.IntegerValue},
					pivot:      values(types.NewIntegerValue(2)),
					val:        func(i int) []types.Value { return values(types.NewIntegerValue(int64(i))) },
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=integer, vals=integers, pivot=double",
					indexTypes:    []types.ValueType{types.IntegerValue},
					pivot:         values(types.NewEmptyValue(types.DoubleValue)),
					val:           func(i int) []types.Value { return values(types.NewIntegerValue(int64(i))) },
					expectedEq:    noCallEq,
					expectedCount: 0,
				},

				// doubles ----------------------------------------------------
				{name: "index=any, vals=doubles, pivot=double",
					indexTypes: nil,
					pivot:      values(types.NewEmptyValue(types.DoubleValue)),
					val:        func(i int) []types.Value { return values(types.NewDoubleValue(float64(i) + float64(i)/2)) },
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							types.NewDoubleValue(float64(i)+float64(i)/2),
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=any, vals=doubles, pivot=double:1.8",
					indexTypes: nil,
					pivot:      values(types.NewDoubleValue(1.8)),
					val:        func(i int) []types.Value { return values(types.NewDoubleValue(float64(i) + float64(i)/2)) },
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							types.NewDoubleValue(float64(i)+float64(i)/2),
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=double, vals=doubles, pivot=double:1.8",
					indexTypes: []types.ValueType{types.DoubleValue},
					pivot:      values(types.NewDoubleValue(1.8)),
					val:        func(i int) []types.Value { return values(types.NewDoubleValue(float64(i) + float64(i)/2)) },
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							types.NewDoubleValue(float64(i)+float64(i)/2),
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=any, vals=doubles, pivot=double:10.8",
					indexTypes:    nil,
					pivot:         values(types.NewDoubleValue(10.8)),
					val:           func(i int) []types.Value { return values(types.NewDoubleValue(float64(i) + float64(i)/2)) },
					expectedEq:    noCallEq,
					expectedCount: 0,
				},

				// text -------------------------------------------------------
				{name: "index=any, vals=text pivot=text",
					indexTypes: nil,
					pivot:      values(types.NewEmptyValue(types.TextValue)),
					val:        func(i int) []types.Value { return values(types.NewTextValue(strconv.Itoa(i))) },
					noise:      noiseInts,
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							types.NewTextValue(strconv.Itoa(int(i))),
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=any, vals=text, pivot=text('2')",
					indexTypes: nil,
					pivot:      values(types.NewTextValue("2")),
					val:        func(i int) []types.Value { return values(types.NewTextValue(strconv.Itoa(i))) },
					noise:      noiseInts,
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							types.NewTextValue(strconv.Itoa(int(i))),
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=any, vals=text, pivot=text('')",
					indexTypes: nil,
					pivot:      values(types.NewTextValue("")),
					val:        func(i int) []types.Value { return values(types.NewTextValue(strconv.Itoa(i))) },
					noise:      noiseInts,
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							types.NewTextValue(strconv.Itoa(int(i))),
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=any, vals=text, pivot=text('foo')",
					indexTypes:    nil,
					pivot:         values(types.NewTextValue("foo")),
					val:           func(i int) []types.Value { return values(types.NewTextValue(strconv.Itoa(i))) },
					noise:         noiseInts,
					expectedEq:    noCallEq,
					expectedCount: 0,
				},
				{name: "index=text, vals=text, pivot=text('2')",
					indexTypes: []types.ValueType{types.TextValue},
					pivot:      values(types.NewTextValue("2")),
					val:        func(i int) []types.Value { return values(types.NewTextValue(strconv.Itoa(i))) },
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							types.NewTextValue(strconv.Itoa(int(i))),
						)(val)
					},
					expectedCount: 3,
				},
				// composite --------------------------------------------------
				// composite indexes can have empty pivot values to iterate on the whole indexed data
				{name: "index=[any, untyped], vals=[int, int], pivot=[nil,nil]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(nil, nil),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 5,
				},

				// composite indexes must have at least have one value if typed
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[int, int]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(types.NewEmptyValue(types.IntegerValue), types.NewEmptyValue(types.IntegerValue)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					expectedEq: noCallEq,
					mustPanic:  true,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[int]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(types.NewEmptyValue(types.IntegerValue)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[0, int, 0]",
					indexTypes: []types.ValueType{0, 0, 0},
					pivot:      values(types.NewIntegerValue(0), types.NewEmptyValue(types.IntegerValue), types.NewIntegerValue(0)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)), types.NewIntegerValue(int64(i+1)))
					},
					expectedEq: noCallEq,
					mustPanic:  true,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[0, int, nil]",
					indexTypes: []types.ValueType{0, 0, 0},
					pivot:      values(types.NewIntegerValue(0), types.NewEmptyValue(types.IntegerValue), types.NewIntegerValue(0), nil),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)), types.NewIntegerValue(int64(i+1)))
					},
					expectedEq: noCallEq,
					mustPanic:  true,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[int, 0]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(types.NewEmptyValue(types.IntegerValue), types.NewIntegerValue(0)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					expectedEq: noCallEq,
					mustPanic:  true,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[0, 0]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(types.NewIntegerValue(0), types.NewIntegerValue(0)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []types.Value {
						return values(types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[2, 0]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(types.NewIntegerValue(2), types.NewIntegerValue(0)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []types.Value {
						return values(types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[2, int]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(types.NewIntegerValue(2), types.NewEmptyValue(types.IntegerValue)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []types.Value {
						return values(types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 3,
				},
				// pivot [2, int] should filter out [2, not(int)]
				{name: "index=[any, untyped], vals=[int, int], noise=[int, blob], pivot=[2, int]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(types.NewIntegerValue(2), types.NewEmptyValue(types.IntegerValue)),
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
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 3,
				},
				// a more subtle case
				{name: "index=[any, untyped], vals=[int, blob], noise=[blob, blob], pivot=[2, 'a']", // pivot is [2, a] but value is [2, c] but that must work anyway
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(types.NewIntegerValue(2), types.NewBlobValue([]byte{byte('a')})),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewBlobValue([]byte{byte('a' + uint8(i))}))
					},
					noise: func(i int) []types.Value {
						return values(types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
							types.NewBlobValue([]byte{byte('a' + uint8(i))}),
						)(val)
					},
					expectedCount: 3,
				},
				// partial pivot
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[0]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(types.NewIntegerValue(0)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						// let's not try to match, it's not important
					},
					expectedCount: 10,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[2]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(types.NewIntegerValue(2)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						// let's not try to match, it's not important
					},
					expectedCount: 6, // total * 2 - (noise + val = 2) * 2
				},
				// this is a tricky test, when we have multiple values but they share the first pivot element;
				// this is by definition a very implementation dependent test.
				{name: "index=[any, untyped], vals=[int, int], noise=int, bool], pivot=[int:0, int:0]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(types.NewIntegerValue(0), types.NewIntegerValue(0)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewBoolValue(true))
					},
					// [0, 0] > [0, true] but [1, true] > [0, 0] so we will see some bools in the results
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						if i%2 == 0 {
							i = i / 2
							requireIdxEncodedEq(t,
								types.NewIntegerValue(int64(i)),
								types.NewIntegerValue(int64(i+1)),
							)(val)
						}
					},
					expectedCount: 9, // 10 elements, but pivot skipped the initial [0, true]
				},
				// index typed
				{name: "index=[int, int], vals=[int, int], pivot=[0, 0]",
					indexTypes: []types.ValueType{types.IntegerValue, types.IntegerValue},
					pivot:      values(types.NewIntegerValue(0), types.NewIntegerValue(0)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=[int, int], vals=[int, int], pivot=[2, 0]",
					indexTypes: []types.ValueType{types.IntegerValue, types.IntegerValue},
					pivot:      values(types.NewIntegerValue(2), types.NewIntegerValue(0)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 3,
				},
				// a more subtle case
				{name: "index=[int, blob], vals=[int, blob], pivot=[2, 'a']", // pivot is [2, a] but value is [2, c] but that must work anyway
					indexTypes: []types.ValueType{types.IntegerValue, types.BlobValue},
					pivot:      values(types.NewIntegerValue(2), types.NewBlobValue([]byte{byte('a')})),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewBlobValue([]byte{byte('a' + uint8(i))}))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
							types.NewBlobValue([]byte{byte('a' + uint8(i))}),
						)(val)
					},
					expectedCount: 3,
				},
				// partial pivot
				{name: "index=[int, int], vals=[int, int], pivot=[0]",
					indexTypes: []types.ValueType{types.IntegerValue, types.IntegerValue},
					pivot:      values(types.NewIntegerValue(0)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=[int, int], vals=[int, int], pivot=[2]",
					indexTypes: []types.ValueType{types.IntegerValue, types.IntegerValue},
					pivot:      values(types.NewIntegerValue(2)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 3,
				},

				// documents --------------------------------------------------
				{name: "index=[any, any], vals=[doc, int], pivot=[{a:2}, 3]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
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
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						requireIdxEncodedEq(t,
							types.NewDocumentValue(testutil.MakeDocument(t, `{"a":`+strconv.Itoa(int(i))+`}`)),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=[document, int], vals=[doc, int], pivot=[{a:2}, 3]",
					indexTypes: []types.ValueType{types.DocumentValue, types.IntegerValue},
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
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						requireIdxEncodedEq(t,
							types.NewDocumentValue(testutil.MakeDocument(t, `{"a":`+strconv.Itoa(int(i))+`}`)),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 3,
				},

				// arrays -----------------------------------------------------
				{name: "index=[any, any], vals=[int[], int], pivot=[]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(),
					val: func(i int) []types.Value {
						return values(
							testutil.MakeArrayValue(t, i, i),
							types.NewIntegerValue(int64(i+1)),
						)
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						requireIdxEncodedEq(t,
							testutil.MakeArrayValue(t, i, i),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=[any, any], vals=[int[], int], pivot=[[2,2], 3]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
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
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						requireIdxEncodedEq(t,
							testutil.MakeArrayValue(t, i, i),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=[any, any], vals=[int[], int[]], pivot=[[2,2], [3,3]]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
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
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						requireIdxEncodedEq(t,
							testutil.MakeArrayValue(t, i, i),
							testutil.MakeArrayValue(t, i+1, i+1),
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=[array, any], vals=[int[], int], pivot=[[2,2], 3]",
					indexTypes: []types.ValueType{types.ArrayValue, types.AnyType},
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
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						requireIdxEncodedEq(t,
							testutil.MakeArrayValue(t, i, i),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 3,
				},
			}

			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					idx, cleanup := getIndex(t, unique, test.indexTypes...)
					defer cleanup()

					for i := 0; i < total; i++ {
						require.NoError(t, idx.Set(test.val(i), []byte{'a' + byte(i)}))
						if test.noise != nil {
							v := test.noise(i)
							if v != nil {
								require.NoError(t, idx.Set(test.noise(i), []byte{'a' + byte(i)}))
							}
						}
					}

					var i uint8
					var count int
					fn := func() error {
						return idx.AscendGreaterOrEqual(test.pivot, func(val, rid []byte) error {
							test.expectedEq(t, i, rid, val)
							i++
							count++
							return nil
						})
					}
					if test.mustPanic {
						// let's avoid panicking because expectedEq wasn't defined, which would
						// be a false positive.
						if test.expectedEq == nil {
							test.expectedEq = func(t *testing.T, i uint8, key, val []byte) {}
						}
						require.Panics(t, func() { _ = fn() })
					} else {
						err := fn()
						require.NoError(t, err)
						require.Equal(t, test.expectedCount, count)
					}
				})
			}
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
			err := idx.AscendGreaterOrEqual(values(types.NewEmptyValue(types.IntegerValue)), func(val, key []byte) error {
				i++
				return errors.New("should not iterate")
			})
			require.NoError(t, err)
			require.Equal(t, 0, i)
		})

		t.Run(text+"Should iterate through documents in order, ", func(t *testing.T) {
			noiseBlob := func(i int) []types.Value {
				t.Helper()
				return []types.Value{types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10))}
			}
			noiseInts := func(i int) []types.Value {
				t.Helper()
				return []types.Value{types.NewIntegerValue(int64(i))}
			}

			noCallEq := func(t *testing.T, i uint8, key []byte, val []byte) {
				require.Fail(t, "equality test should not be called here")
			}

			// the following tests will use that constant to determine how many values needs to be inserted
			// with the value and noise generators.
			total := 5

			tests := []struct {
				name string
				// the index type(s) that is being used
				indexTypes []types.ValueType
				// the pivot, typed or not used to iterate
				pivot database.Pivot
				// the generator for the values that are being indexed
				val func(i int) []types.Value
				// the generator for the noise values that are being indexed
				noise func(i int) []types.Value
				// the function to compare the key/value that the iteration yields
				expectedEq func(t *testing.T, i uint8, key []byte, val []byte)
				// the total count of iteration that should happen
				expectedCount int
				mustPanic     bool
			}{
				// integers ---------------------------------------------------
				{name: "index=any, vals=integers, pivot=integer",
					indexTypes: nil,
					pivot:      values(types.NewEmptyValue(types.IntegerValue)),
					val:        func(i int) []types.Value { return values(types.NewIntegerValue(int64(i))) },
					noise:      noiseBlob,
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						require.Equal(t, []byte{'a' + i}, key)
						requireEqualBinary(t, testutil.MakeArrayValue(t, int64(i)), val)
					},
					expectedCount: 5,
				},
				{name: "index=integer, vals=integers, pivot=integer",
					indexTypes: []types.ValueType{types.IntegerValue},
					pivot:      values(types.NewEmptyValue(types.IntegerValue)),
					val:        func(i int) []types.Value { return values(types.NewIntegerValue(int64(i))) },
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=any, vals=integers, pivot=integer:2",
					indexTypes: nil,
					pivot:      values(types.NewIntegerValue(2)),
					val:        func(i int) []types.Value { return values(types.NewIntegerValue(int64(i))) },
					noise:      noiseBlob,
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 2
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=any, vals=integers, pivot=integer:-10",
					indexTypes:    nil,
					pivot:         values(types.NewIntegerValue(-10)),
					val:           func(i int) []types.Value { return values(types.NewIntegerValue(int64(i))) },
					noise:         noiseBlob,
					expectedEq:    noCallEq,
					expectedCount: 0,
				},
				{name: "index=integer, vals=integers, pivot=integer:2",
					indexTypes: []types.ValueType{types.IntegerValue},
					pivot:      values(types.NewIntegerValue(2)),
					val:        func(i int) []types.Value { return values(types.NewIntegerValue(int64(i))) },
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 2
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=integer, vals=integers, pivot=double",
					indexTypes:    []types.ValueType{types.IntegerValue},
					pivot:         values(types.NewEmptyValue(types.DoubleValue)),
					val:           func(i int) []types.Value { return values(types.NewIntegerValue(int64(i))) },
					expectedEq:    noCallEq,
					expectedCount: 0,
				},

				// doubles ----------------------------------------------------
				{name: "index=any, vals=doubles, pivot=double",
					indexTypes: nil,
					pivot:      values(types.NewEmptyValue(types.DoubleValue)),
					val:        func(i int) []types.Value { return values(types.NewDoubleValue(float64(i) + float64(i)/2)) },
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							types.NewDoubleValue(float64(i)+float64(i)/2),
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=any, vals=doubles, pivot=double:1.8",
					indexTypes: nil,
					pivot:      values(types.NewDoubleValue(1.8)),
					val:        func(i int) []types.Value { return values(types.NewDoubleValue(float64(i) + float64(i)/2)) },
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 3
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							types.NewDoubleValue(float64(i)+float64(i)/2),
						)(val)
					},
					expectedCount: 2,
				},
				{name: "index=double, vals=doubles, pivot=double:1.8",
					indexTypes: []types.ValueType{types.DoubleValue},
					pivot:      values(types.NewDoubleValue(1.8)),
					val:        func(i int) []types.Value { return values(types.NewDoubleValue(float64(i) + float64(i)/2)) },
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 3
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							types.NewDoubleValue(float64(i)+float64(i)/2),
						)(val)
					},
					expectedCount: 2,
				},
				{name: "index=any, vals=doubles, pivot=double:-10.8",
					indexTypes:    nil,
					pivot:         values(types.NewDoubleValue(-10.8)),
					val:           func(i int) []types.Value { return values(types.NewDoubleValue(float64(i) + float64(i)/2)) },
					expectedEq:    noCallEq,
					expectedCount: 0,
				},

				// text -------------------------------------------------------
				{name: "index=any, vals=text pivot=text",
					indexTypes: nil,
					pivot:      values(types.NewEmptyValue(types.TextValue)),
					val:        func(i int) []types.Value { return values(types.NewTextValue(strconv.Itoa(i))) },
					noise:      noiseInts,
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							types.NewTextValue(strconv.Itoa(int(i))),
						)(val)

					},
					expectedCount: 5,
				},
				{name: "index=any, vals=text, pivot=text('2')",
					indexTypes: nil,
					pivot:      values(types.NewTextValue("2")),
					val:        func(i int) []types.Value { return values(types.NewTextValue(strconv.Itoa(i))) },
					noise:      noiseInts,
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 2
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							types.NewTextValue(strconv.Itoa(int(i))),
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=any, vals=text, pivot=text('')",
					indexTypes: nil,
					pivot:      values(types.NewTextValue("")),
					val:        func(i int) []types.Value { return values(types.NewTextValue(strconv.Itoa(i))) },
					noise:      noiseInts,
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							types.NewTextValue(strconv.Itoa(int(i))),
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=any, vals=text, pivot=text('foo')",
					indexTypes: nil,
					pivot:      values(types.NewTextValue("foo")),
					val:        func(i int) []types.Value { return values(types.NewTextValue(strconv.Itoa(i))) },
					noise:      noiseInts,
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							types.NewTextValue(strconv.Itoa(int(i))),
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=text, vals=text, pivot=text('2')",
					indexTypes: []types.ValueType{types.TextValue},
					pivot:      values(types.NewTextValue("2")),
					val:        func(i int) []types.Value { return values(types.NewTextValue(strconv.Itoa(i))) },
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 2
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							types.NewTextValue(strconv.Itoa(int(i))),
						)(val)
					},
					expectedCount: 3,
				},
				// composite --------------------------------------------------
				// composite indexes can have empty pivot values to iterate on the whole indexed data
				{name: "index=[any, untyped], vals=[int, int], pivot=[nil,nil]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(nil, nil),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[int]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(types.NewEmptyValue(types.IntegerValue)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 5,
				},
				// composite indexes cannot have values with type past the first element
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[int, int]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(types.NewEmptyValue(types.IntegerValue), types.NewEmptyValue(types.IntegerValue)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					mustPanic: true,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[0, int, 0]",
					indexTypes: []types.ValueType{0, 0, 0},
					pivot:      values(types.NewIntegerValue(0), types.NewEmptyValue(types.IntegerValue), types.NewIntegerValue(0)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)), types.NewIntegerValue(int64(i+1)))
					},
					mustPanic: true,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[int, 0]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(types.NewEmptyValue(types.IntegerValue), types.NewIntegerValue(0)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					mustPanic: true,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[0, 0]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(types.NewIntegerValue(0), types.NewIntegerValue(0)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []types.Value {
						return values(types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq:    noCallEq,
					expectedCount: 0,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[5, 5]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(types.NewIntegerValue(5), types.NewIntegerValue(5)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []types.Value {
						return values(types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 5,
				},
				// [0,1], [1,2], --[2,0]--,  [2,3], [3,4], [4,5]
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[2, 0]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(types.NewIntegerValue(2), types.NewIntegerValue(0)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []types.Value {
						return values(types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 3
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 2,
				},
				// [0,1], [1,2], [2,3], --[2,int]--, [3,4], [4,5]
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[2, int]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(types.NewIntegerValue(2), types.NewEmptyValue(types.IntegerValue)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []types.Value {
						return values(types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 2
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 3,
				},
				// pivot [2, int] should filter out [2, not(int)]
				// [0,1], [1,2], [2,3], --[2,int]--, [2, text], [3,4], [3,text], [4,5], [4,text]
				{name: "index=[any, untyped], vals=[int, int], noise=[int, text], pivot=[2, int]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(types.NewIntegerValue(2), types.NewEmptyValue(types.IntegerValue)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []types.Value {
						if i > 1 {
							return values(types.NewIntegerValue(int64(i)), types.NewTextValue("foo"))
						}

						return nil
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 2
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 3,
				},
				// a more subtle case
				{name: "index=[any, untyped], vals=[int, blob], noise=[blob, blob], pivot=[2, 'a']", // pivot is [2, a] but value is [2, c] but that must work anyway
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(types.NewIntegerValue(2), types.NewBlobValue([]byte{byte('a')})),
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
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 3
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
							types.NewBlobValue([]byte{byte('a' + uint8(i))}),
						)(val)
					},
					expectedCount: 2,
				},
				// only one of the indexed value is typed
				{name: "index=[any, blob], vals=[int, blob], noise=[blob, blob], pivot=[2, 'a']", // pivot is [2, a] but value is [2, c] but that must work anyway
					indexTypes: []types.ValueType{0, types.BlobValue},
					pivot:      values(types.NewIntegerValue(2), types.NewBlobValue([]byte{byte('a')})),
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
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 3
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
							types.NewBlobValue([]byte{byte('a' + uint8(i))}),
						)(val)
					},
					expectedCount: 2,
				},
				// partial pivot
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[0]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(types.NewIntegerValue(0)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						// let's not try to match, it's not important
					},
					expectedCount: 2, // [0] is "equal" to [0, 1] and [0, "1"]
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[5]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(types.NewIntegerValue(5)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						// let's not try to match, it's not important
					},
					expectedCount: 10,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[2]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(types.NewIntegerValue(2)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						// let's not try to match, it's not important
					},
					expectedCount: 6, // total * 2 - (noise + val = 2) * 2
				},
				// index typed
				{name: "index=[int, int], vals=[int, int], pivot=[0, 0]",
					indexTypes: []types.ValueType{types.IntegerValue, types.IntegerValue},
					pivot:      values(types.NewIntegerValue(0), types.NewIntegerValue(0)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					expectedEq:    noCallEq,
					expectedCount: 0,
				},
				{name: "index=[int, int], vals=[int, int], pivot=[5, 6]",
					indexTypes: []types.ValueType{types.IntegerValue, types.IntegerValue},
					pivot:      values(types.NewIntegerValue(5), types.NewIntegerValue(6)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=[int, int], vals=[int, int], pivot=[2, 0]",
					indexTypes: []types.ValueType{types.IntegerValue, types.IntegerValue},
					pivot:      values(types.NewIntegerValue(2), types.NewIntegerValue(0)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 3
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 2,
				},
				// a more subtle case
				{name: "index=[int, blob], vals=[int, blob], pivot=[2, 'a']", // pivot is [2, a] but value is [2, c] but that must work anyway
					indexTypes: []types.ValueType{types.IntegerValue, types.BlobValue},
					pivot:      values(types.NewIntegerValue(2), types.NewBlobValue([]byte{byte('a')})),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewBlobValue([]byte{byte('a' + uint8(i))}))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 3
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
							types.NewBlobValue([]byte{byte('a' + uint8(i))}),
						)(val)
					},
					expectedCount: 2,
				},
				// partial pivot
				{name: "index=[int, int], vals=[int, int], pivot=[0]",
					indexTypes: []types.ValueType{types.IntegerValue, types.IntegerValue},
					pivot:      values(types.NewIntegerValue(0)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 4
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 1,
				},
				// [0,1], [1,2], [2,3], --[2]--, [3,4], [4,5]
				{name: "index=[int, int], vals=[int, int], pivot=[2]",
					indexTypes: []types.ValueType{types.IntegerValue, types.IntegerValue},
					pivot:      values(types.NewIntegerValue(2)),
					val: func(i int) []types.Value {
						return values(types.NewIntegerValue(int64(i)), types.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 2
						requireIdxEncodedEq(t,
							types.NewIntegerValue(int64(i)),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 3,
				},
				// documents --------------------------------------------------
				{name: "index=[any, any], vals=[doc, int], pivot=[{a:2}, 3]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
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
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 2
						requireIdxEncodedEq(t,
							types.NewDocumentValue(testutil.MakeDocument(t, `{"a":`+strconv.Itoa(int(i))+`}`)),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=[document, int], vals=[doc, int], pivot=[{a:2}, 3]",
					indexTypes: []types.ValueType{types.DocumentValue, types.IntegerValue},
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
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 2
						requireIdxEncodedEq(t,
							types.NewDocumentValue(testutil.MakeDocument(t, `{"a":`+strconv.Itoa(int(i))+`}`)),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 3,
				},

				// arrays -----------------------------------------------------
				{name: "index=[any, any], vals=[int[], int], pivot=[]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
					pivot:      values(),
					val: func(i int) []types.Value {
						return values(
							testutil.MakeArrayValue(t, i, i),
							types.NewIntegerValue(int64(i+1)),
						)
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						requireIdxEncodedEq(t,
							testutil.MakeArrayValue(t, i, i),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=[any, any], vals=[int[], int], pivot=[[2,2], 3]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
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
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 2
						requireIdxEncodedEq(t,
							testutil.MakeArrayValue(t, i, i),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=[any, any], vals=[int[], int[]], pivot=[[2,2], [3,3]]",
					indexTypes: []types.ValueType{types.AnyType, types.AnyType},
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
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 2
						requireIdxEncodedEq(t,
							testutil.MakeArrayValue(t, i, i),
							testutil.MakeArrayValue(t, i+1, i+1),
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=[array, any], vals=[int[], int], pivot=[[2,2], 3]",
					indexTypes: []types.ValueType{types.ArrayValue, types.AnyType},
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
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 2
						requireIdxEncodedEq(t,
							testutil.MakeArrayValue(t, i, i),
							types.NewIntegerValue(int64(i+1)),
						)(val)
					},
					expectedCount: 3,
				},
			}

			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					idx, cleanup := getIndex(t, unique, test.indexTypes...)
					defer cleanup()

					for i := 0; i < total; i++ {
						require.NoError(t, idx.Set(test.val(i), []byte{'a' + byte(i)}))
						if test.noise != nil {
							v := test.noise(i)
							if v != nil {
								require.NoError(t, idx.Set(test.noise(i), []byte{'a' + byte(i)}))
							}
						}
					}

					var i uint8
					var count int

					fn := func() error {
						t.Helper()
						return idx.DescendLessOrEqual(test.pivot, func(val, rid []byte) error {
							test.expectedEq(t, uint8(total-1)-i, rid, val)
							i++
							count++
							return nil
						})
					}
					if test.mustPanic {
						// let's avoid panicking because expectedEq wasn't defined, which would
						// be a false positive.
						if test.expectedEq == nil {
							test.expectedEq = func(t *testing.T, i uint8, key, val []byte) {}
						}
						require.Panics(t, func() {
							_ = fn()
						})
					} else {
						err := fn()
						require.NoError(t, err)
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
				idx, cleanup := getIndex(b, false)

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
			idx, cleanup := getIndex(b, false)
			defer cleanup()

			for i := 0; i < size; i++ {
				k := []byte(fmt.Sprintf("name-%d", i))
				_ = idx.Set(values(types.NewTextValue(string(k))), k)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = idx.AscendGreaterOrEqual(values(types.NewEmptyValue(types.TextValue)), func(_, _ []byte) error {
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
				idx, cleanup := getIndex(b, false, types.TextValue, types.TextValue)

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
			idx, cleanup := getIndex(b, false, types.AnyType, types.AnyType)
			defer cleanup()

			for i := 0; i < size; i++ {
				k := []byte(fmt.Sprintf("name-%d", i))
				_ = idx.Set(values(types.NewTextValue(string(k)), types.NewTextValue(string(k))), k)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = idx.AscendGreaterOrEqual(values(types.NewTextValue(""), types.NewTextValue("")), func(_, _ []byte) error {
					return nil
				})
			}
			b.StopTimer()
		})
	}
}
