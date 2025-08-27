package types

import (
	"github.com/chaisql/chai/internal/encoding"
)

var encodedTypeToTypeDefs = map[byte]TypeDefinition{
	encoding.NullValue:    NullTypeDef{},
	encoding.FalseValue:   BooleanTypeDef{},
	encoding.TrueValue:    BooleanTypeDef{},
	encoding.Int8Value:    IntegerTypeDef{},
	encoding.Int16Value:   IntegerTypeDef{},
	encoding.Int32Value:   IntegerTypeDef{},
	encoding.Int64Value:   BigintTypeDef{},
	encoding.Uint8Value:   IntegerTypeDef{},
	encoding.Uint16Value:  IntegerTypeDef{},
	encoding.Uint32Value:  IntegerTypeDef{},
	encoding.Uint64Value:  BigintTypeDef{},
	encoding.Float64Value: DoubleTypeDef{},
	encoding.TextValue:    TextTypeDef{},
	encoding.BlobValue:    BlobTypeDef{},
}

func DecodeValue(b []byte) (v Value, n int) {
	t := b[0]
	// deal with descending values
	if t > 128 {
		t = 255 - t
	}

	if t >= encoding.IntSmallValue && t < encoding.Uint8Value {
		return IntegerTypeDef{}.Decode(b)
	}

	return encodedTypeToTypeDefs[t].Decode(b)
}

func DecodeValues(b []byte) []Value {
	var values []Value

	for len(b) > 0 {
		v, n := DecodeValue(b)
		values = append(values, v)
		b = b[n:]
	}

	return values
}

func EncodeValuesAsKey(dst []byte, values ...Value) ([]byte, error) {
	var err error

	for _, v := range values {
		dst, err = EncodeValueAsKey(dst, v, false)
		if err != nil {
			return nil, err
		}
	}

	return dst, nil
}

func EncodeValueAsKey(dst []byte, v Value, desc bool) ([]byte, error) {
	newDst, err := v.EncodeAsKey(dst)
	if err != nil {
		return nil, err
	}

	if desc {
		newDst, _ = Desc(newDst, len(newDst)-len(dst))
	}

	return newDst, nil
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
