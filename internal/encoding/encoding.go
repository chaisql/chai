package encoding

import (
	"fmt"
	"time"

	"github.com/chaisql/chai/internal/types"
)

func EncodeBoolean(dst []byte, x bool) []byte {
	if x {
		return append(dst, byte(TrueValue))
	}

	return append(dst, byte(FalseValue))
}

func DecodeBoolean(b []byte) bool {
	return b[0] == byte(TrueValue)
}

func EncodeNull(dst []byte) []byte {
	return append(dst, byte(NullValue))
}

func EncodeValue(dst []byte, v types.Value, desc bool) ([]byte, error) {
	newDst, err := encodeValueAsc(dst, v)
	if err != nil {
		return nil, err
	}

	if desc {
		newDst, _ = Desc(newDst, len(newDst)-len(dst))
	}

	return newDst, nil
}

func encodeValueAsc(dst []byte, v types.Value) ([]byte, error) {
	if v.V() == nil {
		switch v.Type() {
		case types.TypeNull:
			return EncodeNull(dst), nil
		case types.TypeBoolean:
			return EncodeBoolean(dst, false), nil
		case types.TypeInteger:
			return EncodeInt(dst, 0), nil
		case types.TypeDouble:
			return EncodeFloat64(dst, 0), nil
		case types.TypeTimestamp:
			return EncodeTimestamp(dst, time.Time{}), nil
		case types.TypeText:
			return EncodeText(dst, ""), nil
		case types.TypeBlob:
			return EncodeBlob(dst, nil), nil
		case types.TypeArray:
			return EncodeArray(dst, nil)
		case types.TypeObject:
			return EncodeObject(dst, nil)
		default:
			panic(fmt.Sprintf("unsupported type %v", v.Type()))
		}
	}

	switch v.Type() {
	case types.TypeNull:
		return EncodeNull(dst), nil
	case types.TypeBoolean:
		return EncodeBoolean(dst, types.AsBool(v)), nil
	case types.TypeInteger:
		return EncodeInt(dst, types.AsInt64(v)), nil
	case types.TypeDouble:
		return EncodeFloat64(dst, types.AsFloat64(v)), nil
	case types.TypeTimestamp:
		return EncodeTimestamp(dst, types.AsTime(v)), nil
	case types.TypeText:
		return EncodeText(dst, types.AsString(v)), nil
	case types.TypeBlob:
		return EncodeBlob(dst, types.AsByteSlice(v)), nil
	case types.TypeArray:
		return EncodeArray(dst, types.AsArray(v))
	case types.TypeObject:
		return EncodeObject(dst, types.AsObject(v))
	}

	return nil, fmt.Errorf("unsupported value type: %s", v.Type())
}

// Desc changes the type of the encoded value to its descending counterpart.
// It is meant to be used in combination with one of the Encode* functions.
//
//	var buf []byte
//	buf, n = encoding.Desc(encoding.EncodeInt(buf, 10))
func Desc(dst []byte, n int) ([]byte, int) {
	if n == 0 {
		return dst, 0
	}

	dst[len(dst)-n] = 255 - dst[len(dst)-n]
	return dst, n
}

func DecodeValue(b []byte, intAsDouble bool) (types.Value, int) {
	t := b[0]
	// deal with descending values
	if t > 128 {
		t = 255 - t
	}

	if t >= IntSmallValue && t < Uint8Value {
		x, n := DecodeInt(b)
		if intAsDouble {
			return types.NewDoubleValue(float64(x)), n
		}
		return types.NewIntegerValue(x), n
	}

	switch t {
	case NullValue:
		return types.NewNullValue(), 1
	case FalseValue:
		return types.NewBooleanValue(false), 1
	case TrueValue:
		return types.NewBooleanValue(true), 1
	case Int8Value, Int16Value, Int32Value, Int64Value, Uint8Value, Uint16Value, Uint32Value, Uint64Value:
		x, n := DecodeInt(b)
		if intAsDouble {
			return types.NewDoubleValue(float64(x)), n
		}
		return types.NewIntegerValue(x), n
	case Float64Value:
		x := DecodeFloat64(b[1:])
		return types.NewDoubleValue(x), 9
	case TextValue:
		x, n := DecodeText(b)
		return types.NewTextValue(x), n
	case BlobValue:
		x, n := DecodeBlob(b)
		return types.NewBlobValue(x), n
	case ArrayValue:
		a := DecodeArray(b, intAsDouble)
		return types.NewArrayValue(a), SkipArray(b[1:]) + 1
	case ObjectValue:
		d := DecodeObject(b, intAsDouble)
		return types.NewObjectValue(d), SkipObject(b[1:]) + 1
	}

	panic(fmt.Sprintf("unsupported value type: %d", t))
}
