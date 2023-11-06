package encoding_test

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/encoding"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func TestCompare(t *testing.T) {
	tests := []struct {
		k1, k2 string
		cmp    int
	}{
		// empty key
		{`[]`, `[]`, 0},

		// null
		{`[null]`, `[null]`, 0},
		{`[null]`, `[]`, 1},
		{`[]`, `[null]`, -1},

		// booleans
		{`[true]`, `[true]`, 0},
		{`[false]`, `[true]`, -1},
		{`[true]`, `[false]`, 1},
		{`[false]`, `[false]`, 0},

		// ints
		{`[1]`, `[1]`, 0},
		{`[1]`, `[2]`, -1},
		{`[2]`, `[1]`, 1},
		{`[1000000000]`, `[1]`, 33},
		{`[254]`, `[255]`, -1},                             // 2x uint8
		{`[255]`, `[254]`, 1},                              // 2x uint8
		{`[10000]`, `[10001]`, -1},                         // 2x uint16
		{`[10001]`, `[10000]`, 1},                          // 2x uint16
		{`[1000000]`, `[1000001]`, -1},                     // 2x uint32
		{`[1000001]`, `[1000000]`, 1},                      // 2x uint32
		{`[1000000000000000]`, `[1000000000000001]`, -1},   // 2x uint64
		{`[1000000000000001]`, `[1000000000000000]`, 1},    // 2x uint64
		{`[-126]`, `[-127]`, 1},                            // 2x int8
		{`[-127]`, `[-126]`, -1},                           // 2x int8
		{`[-10000]`, `[-10001]`, 1},                        // 2x int16
		{`[-10001]`, `[-10000]`, -1},                       // 2x int16
		{`[-1000000]`, `[-1000001]`, 1},                    // 2x int32
		{`[-1000001]`, `[-1000000]`, -1},                   // 2x int32
		{`[-1000000000000000]`, `[-1000000000000001]`, 1},  // 2x int64
		{`[-1000000000000001]`, `[-1000000000000000]`, -1}, // 2x int64
		{`[-1]`, `[1]`, -2},                                // neg fixint < fixuint
		{`[1]`, `[31]`, -30},                               // neg fixint < fixuint
		{`[-127]`, `[1]`, -34},                             // int8 < fixuint
		{`[-10000]`, `[1]`, -35},                           // int16 < fixuint
		{`[-1000000]`, `[1]`, -36},                         // int32 < fixuint
		{`[-1000000000000000]`, `[1]`, -37},                // int64 < fixuint
		{`[-127]`, `[255]`, -65},                           // int8 < uint8
		{`[-60000]`, `[255]`, -67},                         // int16 < uint8
		{`[-1000000]`, `[255]`, -67},                       // int32 < uint8
		{`[-1000000000000000]`, `[255]`, -68},              // int64 < uint8

		// floats
		{`[1.0]`, `[1.0]`, 0},
		{`[1.1]`, `[1.0]`, 1},
		{`[1.0]`, `[1.1]`, -1},
		{`[-1.0]`, `[-1.1]`, 1},
		// doubles
		{`[1e50]`, `[1e50]`, 0},
		{`[1e51]`, `[1e50]`, 1},
		{`[1e50]`, `[1e51]`, -1},
		{`[-1e50]`, `[-1e51]`, 1},
		// floats and doubles
		{`[1.0]`, `[1e50]`, -1},
		{`[1e50]`, `[1.0]`, 1},

		// text
		{`["a"]`, `["a"]`, 0},
		{`["b"]`, `["a"]`, 1},
		{`["a"]`, `["b"]`, -1},
		{`["a"]`, `["aa"]`, -1},
		{`["aaaa"]`, `["aab"]`, -1},

		// blob
		{`["\xaa"]`, `["\xaa"]`, 0},
		{`["\xab"]`, `["\xaa"]`, 1},
		{`["\xaa"]`, `["\xab"]`, -1},
		{`["\xaa"]`, `["\xaaaa"]`, -1},

		// arrays
		{`[[]]`, `[[]]`, 0},
		{`[[1]]`, `[[]]`, 1},
		{`[[]]`, `[[1]]`, -1},
		{`[[1]]`, `[[1]]`, 0},
		{`[[1]]`, `[[1, 1]]`, -1},

		// maps
		{`[{"a": 2}]`, `[{"a": 2}]`, 0},
		{`[{"a": 1}]`, `[{"a": 2}]`, -1},
		{`[{"a": 2}]`, `[{"a": 1}]`, 1},
		{`[{"a": 1}]`, `[{"b": 1}]`, -1},
		{`[{"b": 1}]`, `[{"a": 1}]`, 1},
		{`[{"a": 1}]`, `[{"a": 1, "b": 1}]`, -1},
		{`[{"a": 1, "b": 1}]`, `[{"a": 1}]`, 1},
		{`[{"a": 1, "b": 1}]`, `[{"a": 1, "b": 1}]`, 0},
		{`[{"a": 1, "b": 1}]`, `[{"a": 1, "b": 2}]`, -1},
		{`[{"a": 1, "b": 2}]`, `[{"a": 1, "b": 1}]`, 1},
		{`[{"a": {"c": [1, 2]}}]`, `[{"a": {"c": [1, 2]}}]`, 0},
		{`[{"a": {"c": [1, 3]}}]`, `[{"a": {"c": [1, 2]}}]`, 1},
		{`[{"a": {"c": []}}]`, `[{"a": {"c": [1, 2]}}]`, -1},

		// different types
		{`[null]`, `[true]`, -4},
		{`[true]`, `[1]`, -43},
		{`[1]`, `[1.0]`, -41},
		{`[1.0]`, `["a"]`, -8},
		{`["a"]`, `["\x00"]`, -5},
		{`["\x00"]`, `[[]]`, -7},
		{`[[]]`, `[{}]`, -10},

		// consecutive values
		{`[1, 2, 3]`, `[1, 2, 3]`, 0},
		{`[1, 2, 3]`, `[1, 2, 4]`, -1},
		{`[1, 2, 3]`, `[1, 2, 3, 4]`, -1},
		// consecutive mixed values
		{`[1, true, 3.4, []]`, `[1, true, 3.4, []]`, 0},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("Compare(%s, %s)", test.k1, test.k2), func(t *testing.T) {
			v1, err := testutil.ParseExpr(t, test.k1).Eval(&environment.Environment{})
			require.NoError(t, err)
			a1 := v1.V().(*document.ValueBuffer).Values
			k1 := mustNewKey(t, a1...)

			v2, err := testutil.ParseExpr(t, test.k2).Eval(&environment.Environment{})
			require.NoError(t, err)
			a2 := v2.V().(*document.ValueBuffer).Values
			k2 := mustNewKey(t, a2...)

			require.Equal(t, test.cmp, encoding.Compare(k1, k2))

			// compare abbreviated keys

			// prepend namespace
			kk1 := mustNewKey(t, append([]types.Value{types.NewIntegerValue(1)}, a1...)...)
			kk2 := mustNewKey(t, append([]types.Value{types.NewIntegerValue(1)}, a2...)...)

			cmp := int64(encoding.AbbreviatedKey(kk1) - encoding.AbbreviatedKey(kk2))
			if test.cmp < 0 {
				require.False(t, cmp > 0)
			} else if test.cmp > 0 {
				require.False(t, cmp < 0)
			} else {
				require.True(t, cmp == 0)
			}
		})
	}
}

func TestAbbreviatedKey(t *testing.T) {
	i64 := int64(-5000000000)
	i32 := int32(-60000000)
	i16 := int16(-10000)
	i8 := int8(-127)
	tests := []struct {
		k    string
		want uint64
	}{
		// empty key
		{`[]`, 0},
		// namespace only
		{`[1]`, 0b_0000000000000001_00000000_0000000000000000000000000000000000000000},
		{`[400]`, 0b_0000000110010000_00000000_0000000000000000000000000000000000000000},
		{`[1000000]`, 0b_1111111111111111_00000000_0000000000000000000000000000000000000000}, // > 1 << 16

		// null
		{`[1, null]`, 1<<48 | uint64(encoding.NullValue)<<40},

		// bool
		{`[1, false]`, 1<<48 | uint64(encoding.FalseValue)<<40},
		{`[1, true]`, 1<<48 | uint64(encoding.TrueValue)<<40},

		// int
		{`[1, 1]`, 1<<48 | (uint64(encoding.IntSmallValue)+32+1)<<40},                                     // positive int -> small value
		{`[1, -10]`, 1<<48 | (uint64(encoding.IntSmallValue)+32-10)<<40},                                  // negative int -> small value
		{`[1, 31]`, 1<<48 | (uint64(encoding.IntSmallValue)+32+31)<<40},                                   // positive int -> small value
		{`[1, 100]`, 1<<48 | uint64(encoding.Uint8Value)<<40 | 100},                                       // positive int -> uint8
		{`[1, 128]`, 1<<48 | uint64(encoding.Uint8Value)<<40 | 128},                                       // positive int -> uint8
		{`[1, 255]`, 1<<48 | uint64(encoding.Uint8Value)<<40 | 255},                                       // positive int -> uint8
		{`[1, 256]`, 1<<48 | uint64(encoding.Uint16Value)<<40 | 256},                                      // positive int -> uint16
		{`[1, 999]`, 1<<48 | uint64(encoding.Uint16Value)<<40 | 999},                                      // positive int -> uint16
		{`[1, -5000000000]`, 1<<48 | uint64(encoding.Int64Value)<<40 | (uint64(i64)+math.MaxInt64+1)>>24}, // int64
		{`[1, -60000000]`, 1<<48 | uint64(encoding.Int32Value)<<40 | (uint64(i32) + math.MaxInt32 + 1)},   // int32
		{`[1, -10000]`, 1<<48 | uint64(encoding.Int16Value)<<40 | (uint64(i16) + math.MaxInt16 + 1)},      // int16
		{`[1, -127]`, 1<<48 | uint64(encoding.Int8Value)<<40 | (uint64(i8) + math.MaxInt8 + 1)},           // int8
		{`[1, 255]`, 1<<48 | uint64(encoding.Uint8Value)<<40 | 255},                                       // uint8
		{`[1, 50000]`, 1<<48 | uint64(encoding.Uint16Value)<<40 | 50000},                                  // uint16
		{`[1, 500000]`, 1<<48 | uint64(encoding.Uint32Value)<<40 | 500000},                                // uint32
		{`[1, 5000000000]`, 1<<48 | uint64(encoding.Uint64Value)<<40 | 5000000000>>24},                    // uint64

		// float / double
		{`[1, 1.0]`, 1<<48 | uint64(encoding.Float64Value)<<40 | uint64(math.Float64bits(1)^(1<<63))>>24},
		{`[1, -1.0]`, 1<<48 | uint64(encoding.Float64Value)<<40 | uint64(math.Float64bits(-1)^(1<<64-1))>>24},
		{`[1, 1e50]`, 1<<48 | uint64(encoding.Float64Value)<<40 | uint64(math.Float64bits(1e50)^1<<63)>>24},
		{`[1, -1e50]`, 1<<48 | uint64(encoding.Float64Value)<<40 | uint64(math.Float64bits(-1e50)^(1<<64-1))>>24},

		// text
		{`[1, "abc"]`, 1<<48 | uint64(encoding.TextValue)<<40 | uint64('a')<<32 | uint64('b')<<24 | uint64('c')<<16},
		{`[1, "abcdefghijkl"]`, 1<<48 | uint64(encoding.TextValue)<<40 | uint64('a')<<32 | uint64('b')<<24 | uint64('c')<<16 | uint64('d')<<8 | uint64('e')},
		{`[1, "abcdefghijkl` + strings.Repeat("m", 100) + `"]`, 1<<48 | uint64(encoding.TextValue)<<40 | uint64('a')<<32 | uint64('b')<<24 | uint64('c')<<16 | uint64('d')<<8 | uint64('e')},
		{`[1, "abcdefghijkl` + strings.Repeat("m", 10000) + `"]`, 1<<48 | uint64(encoding.TextValue)<<40 | uint64('a')<<32 | uint64('b')<<24 | uint64('c')<<16 | uint64('d')<<8 | uint64('e')},

		// blob
		{`[1, "\xab"]`, 1<<48 | uint64(encoding.BlobValue)<<40 | uint64(0xab)<<32},
		{`[1, "\xabcdefabcdef"]`, 1<<48 | uint64(encoding.BlobValue)<<40 | uint64(0xab)<<32 | uint64(0xcd)<<24 | uint64(0xef)<<16 | uint64(0xab)<<8 | uint64(0xcd)},
		{`[1, "\xabcdefabcdef` + strings.Repeat("c", 100) + `"]`, 1<<48 | uint64(encoding.BlobValue)<<40 | uint64(0xab)<<32 | uint64(0xcd)<<24 | uint64(0xef)<<16 | uint64(0xab)<<8 | uint64(0xcd)},
		{`[1, "\xabcdefabcdef` + strings.Repeat("c", 1000) + `"]`, 1<<48 | uint64(encoding.BlobValue)<<40 | uint64(0xab)<<32 | uint64(0xcd)<<24 | uint64(0xef)<<16 | uint64(0xab)<<8 | uint64(0xcd)},

		// array
		{`[1, []]`, 1<<48 | uint64(encoding.ArrayValue)<<40},
		{`[1, [1, 1]]`, 1<<48 | uint64(encoding.ArrayValue)<<40 | (uint64(encoding.IntSmallValue)+32+1)<<32},
		{`[1, [[]]]`, 1<<48 | uint64(encoding.ArrayValue)<<40 | uint64(encoding.ArrayValue)<<32},
		// doc
		{`[1, {}]`, 1<<48 | uint64(encoding.DocumentValue)<<40},
		{`[1, {a: 1}]`, 1<<48 | uint64(encoding.DocumentValue)<<40 | uint64(encoding.TextValue)<<32 | uint64('a')<<24},
		{`[1, {a: 2}]`, 1<<48 | uint64(encoding.DocumentValue)<<40 | uint64(encoding.TextValue)<<32 | uint64('a')<<24},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("AbbreviatedKey(%s)", test.k), func(t *testing.T) {
			v, err := testutil.ParseExpr(t, test.k).Eval(&environment.Environment{})
			require.NoError(t, err)
			a := v.V().(*document.ValueBuffer).Values
			k := mustNewKey(t, a...)

			require.Equal(t, test.want, encoding.AbbreviatedKey(k))
		})
	}
}

func TestSeparator(t *testing.T) {
	tests := []struct {
		k1, k2 string
	}{
		{`[1, 1]`, `[1, 2]`},
		{`[1, 1]`, `[1, 3]`},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("Separator(%v, %v)", test.k1, test.k2), func(t *testing.T) {
			v1, err := testutil.ParseExpr(t, test.k1).Eval(&environment.Environment{})
			require.NoError(t, err)
			v2, err := testutil.ParseExpr(t, test.k2).Eval(&environment.Environment{})
			require.NoError(t, err)
			k1 := mustNewKey(t, v1.V().(*document.ValueBuffer).Values...)
			k2 := mustNewKey(t, v2.V().(*document.ValueBuffer).Values...)
			sep := encoding.Separator(nil, k1, k2)
			require.LessOrEqual(t, encoding.Compare(k1, sep), 0)
			require.Less(t, encoding.Compare(sep, k2), 0)
		})
	}
}
