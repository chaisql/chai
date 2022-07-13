package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

func write1(dst []byte, code byte, n uint8) []byte {
	return append(dst, code, n)
}

func write2(dst []byte, code byte, n uint16) []byte {
	return append(dst, code, byte(n>>8), byte(n))
}

func write4(dst []byte, code byte, n uint32) []byte {
	return append(
		dst,
		code,
		byte(n>>24),
		byte(n>>16),
		byte(n>>8),
		byte(n),
	)
}

func write8(dst []byte, code byte, n uint64) []byte {
	return append(
		dst,
		code,
		byte(n>>56),
		byte(n>>48),
		byte(n>>40),
		byte(n>>32),
		byte(n>>24),
		byte(n>>16),
		byte(n>>8),
		byte(n),
	)
}

func Skip(b []byte) int {
	if b[0] >= IntSmallValue && b[0] < Uint8Value {
		return 1
	}

	switch b[0] {
	case NullValue, FalseValue, TrueValue:
		return 1
	case Int8Value, Uint8Value:
		return 2
	case Int16Value, Uint16Value:
		return 3
	case Int32Value, Uint32Value, Float32Value:
		return 5
	case Int64Value, Uint64Value, Float64Value:
		return 9
	case TextValue, BlobValue:
		l, n := binary.Uvarint(b[1:])
		return n + int(l) + 1
	case ArrayValue:
		return 1 + SkipArray(b[1:])
	case DocumentValue:
		return 1 + SkipDocument(b[1:])
	}

	panic("unreachable")
}

func SkipArray(b []byte) int {
	l, n := binary.Uvarint(b)

	for i := 0; i < int(l); i++ {
		n += Skip(b[n:])
	}

	return n
}

func SkipDocument(b []byte) int {
	l, n := binary.Uvarint(b)

	for i := 0; i < int(l); i++ {
		// skip field
		n += Skip(b[n:])

		// skip value
		n += Skip(b[n:])
	}

	return n
}

func Equal(a, b []byte) bool {
	return bytes.Equal(a, b)
}

func Compare(a, b []byte) int {
	var n, cmp int

	for {
		if n == len(a) {
			if len(b) == n {
				return 0
			}
			return -1
		} else if n == len(b) {
			return 1
		}

		a = a[n:]
		b = b[n:]

		cmp, n = compareNextValue(a, b)
		if cmp != 0 {
			return cmp
		}
	}
}

func compareNextValue(a, b []byte) (cmp int, n int) {
	if len(a) == 0 || len(b) == 0 {
		if len(a) == 0 && len(b) == 0 {
			return 0, 0
		}

		if len(a) == 0 {
			return -1, 0
		}

		return 1, 0
	}

	// compare the type first
	cmp = int(a[0]) - int(b[0])
	if cmp != 0 {
		return cmp, 1
	}

	if a[0] >= IntSmallValue && a[0] < Uint8Value {
		return 0, 1
	}

	// then compare values
	switch a[0] {
	case NullValue, FalseValue, TrueValue:
		fallthrough
	case 0: // tombstone
		return 0, 1
	}

	// deal with empty values
	if len(a) == 1 || len(b) == 1 {
		if len(a) == 1 && len(b) > 1 {
			return -1, 1
		}

		if len(a) > 1 && len(b) == 1 {
			return 1, 1
		}

		return 0, 1
	}

	// compare non empty values
	switch a[0] {
	case Int64Value, Uint64Value, Float64Value:
		return bytes.Compare(a[1:9], b[1:9]), 9
	case Int32Value, Uint32Value, Float32Value:
		return bytes.Compare(a[1:5], b[1:5]), 5
	case Int16Value, Uint16Value:
		return bytes.Compare(a[1:3], b[1:3]), 3
	case Int8Value, Uint8Value:
		return bytes.Compare(a[1:2], b[1:2]), 2
	case TextValue, BlobValue:
		l, n := binary.Uvarint(a[1:])
		n++
		enda := n + int(l)
		l, n = binary.Uvarint(b[1:])
		n++
		endb := n + int(l)
		return bytes.Compare(a[n:enda], b[n:endb]), enda
	case ArrayValue:
		la, _ := binary.Uvarint(a[1:])
		lb, n := binary.Uvarint(b[1:])
		minl := la
		if lb < minl {
			minl = lb
		}
		n++
		for i := 0; i < int(minl); i++ {
			cmp, nn := compareNextValue(a[n:], b[n:])
			n += nn
			if cmp != 0 {
				return cmp, n
			}
		}
		if la < lb {
			return -1, n
		}
		if la > lb {
			return 1, n
		}

		return 0, n
	case DocumentValue:
		la, _ := binary.Uvarint(a[1:])
		lb, n := binary.Uvarint(b[1:])
		minl := la
		if lb < minl {
			minl = lb
		}
		n++
		for i := 0; i < int(minl); i++ {
			// compare field
			cmp, nn := compareNextValue(a[n:], b[n:])
			n += nn
			if cmp != 0 {
				return cmp, n
			}

			// compare value
			cmp, nn = compareNextValue(a[n:], b[n:])
			n += nn
			if cmp != 0 {
				return cmp, n
			}
		}
		if la < lb {
			return -1, n
		}
		if la > lb {
			return 1, n
		}

		return 0, n
	}

	panic(fmt.Sprintf("unsupported value type: %d", a[0]))
}

func Successor(dst, a []byte) []byte {
	if len(a) == 0 {
		return a
	}

	namespace, _ := DecodeInt(a)
	if namespace == math.MaxInt64 {
		return a
	}
	namespace++
	return EncodeInt(dst, namespace)
}

// AbbreviatedKey returns a shortened version that is used for
// comparing keys during indexed batch comparisons.
// The key is not guaranteed to be unique, but it respects the
// same ordering as the original key.
// If two abbreviated keys are equal, Pebble will call the
// Equal function to determine if the original keys are equal.
// The key is constructed as follows:
// - 12 bits: the namespace, from 0 to 4096. If bigger than 4096, returns math.MaxUint64.
// - 4 bits: the Genji type of the first value.
// - 48 bits: a representation of the first value of the key, depending on its type.
func AbbreviatedKey(key []byte) uint64 {
	if len(key) == 0 {
		return 0
	}

	var abbv uint64

	// get the namespace
	namespace, n := DecodeInt(key)
	key = key[n:]
	if namespace >= 1<<16 {
		return math.MaxUint16 << 48
	}

	// First 16 bits are the namespace. (64 - 16 = 48)
	abbv |= uint64(namespace) << 48

	if len(key) == 0 {
		return abbv
	}

	// Get a sorted int value from the key.
	// The type is encoded on 8 bits
	tn := key[0]

	// Set the type. (48 - 8 = 40)
	abbv |= uint64(tn) << 40

	abbv |= abbreviatedValue(key)
	return abbv
}

// return the abbreviated value of the first value on max 5 bytes.
func abbreviatedValue(key []byte) uint64 {
	if len(key) == 0 {
		return 0
	}

	if key[0] >= IntSmallValue && key[0] < Uint8Value {
		return 0
	}

	switch key[0] {
	case NullValue:
		return 0
	case TrueValue, FalseValue:
		return 0
	case Uint8Value, Int8Value:
		x := DecodeUint8(key[1:])
		return uint64(x)
	case Uint16Value, Int16Value:
		x := DecodeUint16(key[1:])
		return uint64(x)
	case Uint32Value, Int32Value:
		x := DecodeUint32(key[1:])
		return uint64(x)
	case Uint64Value, Int64Value, Float64Value:
		x := DecodeUint64(key[1:])
		return uint64(x) >> 24
	case TextValue, BlobValue:
		var abbv uint64
		l, n := binary.Uvarint(key[1:])
		n++
		key = key[n:]
		ll := int(l)
		// put the first 5 bytes of the value
		for i := 0; i < 5 && i < ll; i++ {
			abbv |= uint64(key[i]) << (32 - uint64(i)*8)
		}
		return abbv
	case ArrayValue, DocumentValue:
		key = key[1:]
		l, n := binary.Uvarint(key)
		key = key[n:]
		if l > 0 {
			switch key[0] {
			case ArrayValue, DocumentValue:
				return uint64(key[0]) << 32
			default:
				abbv := uint64(key[0]) << 32
				x := abbreviatedValue(key) >> 8
				return abbv | x
			}
		}

		return 0
	}

	return 0
}

func Separator(dst, a, b []byte) (foo []byte) {
	var n, cmp int
	var idx int
	var aa []byte = a

	for {
		if n == len(a) {
			return a
		}

		a = a[n:]
		b = b[n:]
		idx += n

		cmp, n = compareNextValue(a, b)
		if cmp != 0 {
			idx += n
			dst = append(dst, aa[:idx]...)
			dst = append(dst, 0xFF)
			return dst
		}
	}
}
