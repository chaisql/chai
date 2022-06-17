package encoding

import (
	"fmt"
	"math"
)

func EncodeInt(dst []byte, n int64) []byte {
	if n >= 0 {
		return EncodeUint(dst, uint64(n))
	}

	if n >= -32 {
		return append(dst, byte(n+int64(IntSmallValue)+32))
	}

	if n >= math.MinInt8 {
		return EncodeInt8(dst, int8(n))
	}
	if n >= math.MinInt16 {
		return EncodeInt16(dst, int16(n))
	}
	if n >= math.MinInt32 {
		return EncodeInt32(dst, int32(n))
	}
	return EncodeInt64(dst, n)
}

func EncodeUint(dst []byte, n uint64) []byte {
	if n <= 127 {
		return append(dst, byte(n+uint64(IntSmallValue)+32))
	}

	if n <= math.MaxUint8 {
		return EncodeUint8(dst, uint8(n))
	}
	if n <= math.MaxUint16 {
		return EncodeUint16(dst, uint16(n))
	}
	if n <= math.MaxUint32 {
		return EncodeUint32(dst, uint32(n))
	}
	return EncodeUint64(dst, n)
}

func EncodeUint8(dst []byte, n uint8) []byte {
	return write1(dst, byte(Uint8Value), n)
}

func EncodeUint16(dst []byte, n uint16) []byte {
	return write2(dst, byte(Uint16Value), n)
}

func EncodeUint32(dst []byte, n uint32) []byte {
	return write4(dst, byte(Uint32Value), n)
}

func EncodeUint64(dst []byte, n uint64) []byte {
	return write8(dst, byte(Uint64Value), n)
}

func EncodeInt8(dst []byte, n int8) []byte {
	return write1(dst, byte(Int8Value), uint8(n)+math.MaxInt8+1)
}

func EncodeInt16(dst []byte, n int16) []byte {
	return write2(dst, byte(Int16Value), uint16(n)+math.MaxInt16+1)
}

func EncodeInt32(dst []byte, n int32) []byte {
	return write4(dst, byte(Int32Value), uint32(n)+math.MaxInt32+1)
}

func EncodeInt64(dst []byte, n int64) []byte {
	return write8(dst, byte(Int64Value), uint64(n)+math.MaxInt64+1)
}

func DecodeInt(b []byte) (int64, int) {
	if b[0] >= IntSmallValue && b[0] <= IntSmallValue+0x9F {
		return int64(int8(b[0] - IntSmallValue - 32)), 1
	}

	switch b[0] {
	case Uint8Value:
		return int64(DecodeUint8(b[1:])), 2
	case Uint16Value:
		return int64(DecodeUint16(b[1:])), 3
	case Uint32Value:
		return int64(DecodeUint32(b[1:])), 5
	case Uint64Value:
		return int64(DecodeUint64(b[1:])), 9
	case Int8Value:
		return int64(DecodeInt8(b[1:])), 2
	case Int16Value:
		return int64(DecodeInt16(b[1:])), 3
	case Int32Value:
		return int64(DecodeInt32(b[1:])), 5
	case Int64Value:
		return DecodeInt64(b[1:]), 9
	}

	panic(fmt.Sprintf("invalid type %0x", b[0]))
}

func DecodeUint8(b []byte) uint8 {
	return b[0]
}

func DecodeUint16(b []byte) uint16 {
	return (uint16(b[0]) << 8) | uint16(b[1])
}

func DecodeUint32(b []byte) uint32 {
	return (uint32(b[0]) << 24) |
		(uint32(b[1]) << 16) |
		(uint32(b[2]) << 8) |
		uint32(b[3])
}

func DecodeUint64(b []byte) uint64 {
	return (uint64(b[0]) << 56) |
		(uint64(b[1]) << 48) |
		(uint64(b[2]) << 40) |
		(uint64(b[3]) << 32) |
		(uint64(b[4]) << 24) |
		(uint64(b[5]) << 16) |
		(uint64(b[6]) << 8) |
		uint64(b[7])
}

func DecodeInt8(b []byte) int8 {
	x := uint8(b[0])
	x -= math.MaxInt8 + 1
	return int8(x)
}

func DecodeInt16(b []byte) int16 {
	x := DecodeUint16(b)
	x -= math.MaxInt16 + 1
	return int16(x)
}

func DecodeInt32(b []byte) int32 {
	x := DecodeUint32(b)
	x -= math.MaxInt32 + 1
	return int32(x)
}

func DecodeInt64(b []byte) int64 {
	x := DecodeUint64(b)
	x -= math.MaxInt64 + 1
	return int64(x)
}

func EncodeFloat(dst []byte, x float64) []byte {
	if float64(int64(x)) == x {
		return EncodeInt(dst, int64(x))
	}

	return EncodeFloat64(dst, x)
}

func EncodeFloat64(dst []byte, x float64) []byte {
	fb := math.Float64bits(x)
	if x >= 0 {
		fb ^= 1 << 63
	} else {
		fb ^= 1<<64 - 1
	}
	return write8(dst, byte(Float64Value), fb)
}

func DecodeFloat(b []byte) (float64, int) {
	switch b[0] {
	case Float64Value:
		return DecodeFloat64(b[1:]), 9
	default:
		x, n := DecodeInt(b)
		return float64(x), n
	}
}

func DecodeFloat64(b []byte) float64 {
	x := DecodeUint64(b)

	if (x & (1 << 63)) != 0 {
		x ^= 1 << 63
	} else {
		x ^= 1<<64 - 1
	}
	return math.Float64frombits(x)
}
