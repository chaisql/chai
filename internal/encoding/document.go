package encoding

import (
	"encoding/binary"
	"fmt"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/types"
)

func EncodeDocument(dst []byte, d types.Document) ([]byte, error) {
	if d == nil {
		dst = EncodeDocumentLength(dst, 0)
		return dst, nil
	}

	l, err := document.Length(d)
	if err != nil {
		return nil, err
	}

	// encode the length as a varint
	dst = EncodeDocumentLength(dst, l)

	fields := make(map[string]struct{}, l)

	err = d.Iterate(func(k string, v types.Value) error {
		if _, ok := fields[k]; ok {
			return fmt.Errorf("duplicate field %s", k)
		}
		fields[k] = struct{}{}

		dst = EncodeText(dst, k)

		dst, err = EncodeValue(dst, v)
		return err
	})
	if err != nil {
		return nil, err
	}

	return dst, nil
}

func EncodeDocumentLength(dst []byte, l int) []byte {
	// encode the length as a varint
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = DocumentValue
	n := binary.PutUvarint(buf[1:], uint64(l))
	return append(dst, buf[:n+1]...)
}

func DecodeDocument(b []byte, intAsDouble bool) types.Document {
	return &EncodedDocument{
		Encoded:     b[1:],
		intAsDouble: intAsDouble,
	}
}

type EncodedDocument struct {
	Encoded     []byte
	intAsDouble bool
}

func (e *EncodedDocument) Iterate(fn func(k string, v types.Value) error) error {
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

func (e *EncodedDocument) GetByField(field string) (types.Value, error) {
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

func (e *EncodedDocument) MarshalJSON() ([]byte, error) {
	return document.MarshalJSON(e)
}
