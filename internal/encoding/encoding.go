package encoding

import (
	"fmt"

	"github.com/genjidb/genji/types"
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

func EncodeValue(dst []byte, v types.Value) ([]byte, error) {
	if v.V() == nil {
		return append(dst, byte(v.Type())), nil
	}

	switch v.Type() {
	case types.NullValue:
		return EncodeNull(dst), nil
	case types.BooleanValue:
		return EncodeBoolean(dst, types.As[bool](v)), nil
	case types.IntegerValue:
		return EncodeInt(dst, types.As[int64](v)), nil
	case types.DoubleValue:
		return EncodeFloat64(dst, types.As[float64](v)), nil
	case types.TextValue:
		return EncodeText(dst, types.As[string](v)), nil
	case types.BlobValue:
		return EncodeBlob(dst, types.As[[]byte](v)), nil
	case types.ArrayValue:
		return EncodeArray(dst, types.As[types.Array](v))
	case types.DocumentValue:
		return EncodeDocument(dst, types.As[types.Document](v))
	}

	return nil, fmt.Errorf("unsupported value type: %s", v.Type())
}

func DecodeValue(b []byte, intAsDouble bool) (types.Value, int) {
	if b[0] >= IntSmallValue && b[0] < Uint8Value {
		x, n := DecodeInt(b)
		if intAsDouble {
			return types.NewDoubleValue(float64(x)), n
		}
		return types.NewIntegerValue(x), n
	}

	switch b[0] {
	case NullValue:
		return types.NewNullValue(), 1
	case FalseValue:
		return types.NewBoolValue(false), 1
	case TrueValue:
		return types.NewBoolValue(true), 1
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
	case DocumentValue:
		d := DecodeDocument(b, intAsDouble)
		return types.NewDocumentValue(d), SkipDocument(b[1:]) + 1
	}

	panic(fmt.Sprintf("unsupported value type: %d", b[0]))
}
