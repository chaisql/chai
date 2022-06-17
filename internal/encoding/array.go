package encoding

import (
	"encoding/binary"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/types"
)

func EncodeArray(dst []byte, a types.Array) ([]byte, error) {
	if a == nil {
		dst = EncodeArrayLength(dst, 0)
		return dst, nil
	}

	l, err := document.ArrayLength(a)
	if err != nil {
		return nil, err
	}
	if l == 0 {
		return append(dst, byte(ArrayValue), 0), nil
	}

	dst = EncodeArrayLength(dst, l)

	err = a.Iterate(func(i int, value types.Value) error {
		dst, err = EncodeValue(dst, value)
		return err
	})
	if err != nil {
		return nil, err
	}

	return dst, nil
}

func EncodeArrayLength(dst []byte, l int) []byte {
	// encode the length as a varint
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = ArrayValue
	n := binary.PutUvarint(buf[1:], uint64(l))
	return append(dst, buf[:n+1]...)
}

func DecodeArray(b []byte, intAsDouble bool) types.Array {
	return &EncodedArray{
		enc:         b[1:],
		intAsDouble: intAsDouble,
	}
}

// An EncodedArray implements the types.Array interface on top of an
// encoded representation of an array.
// It is useful for avoiding decoding the entire array when
// only a few values are needed.
type EncodedArray struct {
	enc         []byte
	intAsDouble bool
}

// Iterate goes through all the values of the array and calls the
// given function by passing each one of them.
// If the given function returns an error, the iteration stops.
func (e *EncodedArray) Iterate(fn func(i int, value types.Value) error) error {
	l, n := binary.Uvarint(e.enc)
	if l == 0 {
		return nil
	}
	b := e.enc[n:]

	ll := int(l)
	for i := 0; i < ll; i++ {
		v, n := DecodeValue(b, e.intAsDouble)
		b = b[n:]

		err := fn(i, v)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetByIndex returns a value by index of the array.
func (e *EncodedArray) GetByIndex(idx int) (v types.Value, err error) {
	l, n := binary.Uvarint(e.enc)
	if l == 0 {
		return nil, types.ErrValueNotFound
	}
	b := e.enc[n:]

	ll := int(l)
	for i := 0; i < ll; i++ {
		if i == idx {
			v, _ := DecodeValue(b, e.intAsDouble)
			return v, nil
		}

		n = Skip(b)
		b = b[n:]
	}

	err = types.ErrValueNotFound
	return
}

func (e *EncodedArray) MarshalJSON() ([]byte, error) {
	return document.MarshalJSONArray(e)
}
