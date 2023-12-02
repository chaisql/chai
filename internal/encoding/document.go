package encoding

import (
	"encoding/binary"
	"fmt"

	"github.com/chaisql/chai/internal/object"
	"github.com/chaisql/chai/internal/types"
)

func EncodeObject(dst []byte, d types.Object) ([]byte, error) {
	if d == nil {
		dst = EncodeObjectLength(dst, 0)
		return dst, nil
	}

	l, err := object.Length(d)
	if err != nil {
		return nil, err
	}

	// encode the length as a varint
	dst = EncodeObjectLength(dst, l)

	fields := make(map[string]struct{}, l)

	err = d.Iterate(func(k string, v types.Value) error {
		if _, ok := fields[k]; ok {
			return fmt.Errorf("duplicate field %s", k)
		}
		fields[k] = struct{}{}

		dst = EncodeText(dst, k)

		dst, err = EncodeValue(dst, v, false)
		return err
	})
	if err != nil {
		return nil, err
	}

	return dst, nil
}

func EncodeObjectLength(dst []byte, l int) []byte {
	// encode the length as a varint
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = ObjectValue
	n := binary.PutUvarint(buf[1:], uint64(l))
	return append(dst, buf[:n+1]...)
}

func DecodeObject(b []byte, intAsDouble bool) types.Object {
	return &EncodedObject{
		Encoded:     b[1:],
		intAsDouble: intAsDouble,
	}
}

type EncodedObject struct {
	Encoded     []byte
	intAsDouble bool
}

func (e *EncodedObject) Iterate(fn func(k string, v types.Value) error) error {
	l, n := binary.Uvarint(e.Encoded)
	if l == 0 {
		return nil
	}
	b := e.Encoded[n:]

	ll := int(l)
	for i := 0; i < ll; i++ {
		k, n := DecodeText(b)
		b = b[n:]

		v, n := DecodeValue(b, e.intAsDouble)
		b = b[n:]

		err := fn(k, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *EncodedObject) GetByField(field string) (types.Value, error) {
	l, n := binary.Uvarint(e.Encoded)
	if l == 0 {
		return nil, types.ErrFieldNotFound
	}
	b := e.Encoded[n:]

	ll := int(l)
	for i := 0; i < ll; i++ {
		k, n := DecodeText(b)
		b = b[n:]

		if k == field {
			v, _ := DecodeValue(b, e.intAsDouble)
			return v, nil
		}

		n = Skip(b)
		b = b[n:]
	}

	return nil, types.ErrFieldNotFound
}

func (e *EncodedObject) MarshalJSON() ([]byte, error) {
	return object.MarshalJSON(e)
}
