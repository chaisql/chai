package tree

import (
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/encoding"
	"github.com/genjidb/genji/types"
)

type Key struct {
	Values  []types.Value
	Encoded []byte
}

func NewKey(values ...types.Value) *Key {
	return &Key{
		Values: values,
	}
}

func NewEncodedKey(enc []byte) *Key {
	return &Key{
		Encoded: enc,
	}
}

func (k *Key) Encode(ns Namespace) ([]byte, error) {
	if k.Encoded != nil {
		return k.Encoded, nil
	}

	var buf []byte
	var err error

	if ns != 0 {
		buf = encoding.EncodeInt(buf, int64(ns))
	}

	for _, v := range k.Values {
		buf, err = encoding.EncodeValue(buf, v)
		if err != nil {
			return nil, err
		}
	}

	k.Encoded = buf
	return buf, nil
}

func (key *Key) Decode() ([]types.Value, error) {
	if key.Values != nil {
		return key.Values, nil
	}

	var values []types.Value

	b := key.Encoded

	// ignore namespace
	n := encoding.Skip(key.Encoded)
	b = b[n:]

	for {
		v, n := encoding.DecodeValue(b, false /* intAsDouble */)
		b = b[n:]

		values = append(values, v)
		if len(b) == 0 {
			break
		}
	}

	return values, nil
}

func (k *Key) String() string {
	values, _ := k.Decode()

	return types.NewArrayValue(document.NewValueBuffer(values...)).String()
}
