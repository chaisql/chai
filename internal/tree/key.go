package tree

import (
	"github.com/cockroachdb/errors"
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

func (k *Key) Encode(ns Namespace, order SortOrder) ([]byte, error) {
	if k.Encoded != nil {
		return k.Encoded, nil
	}

	var buf []byte
	var err error

	if ns != 0 {
		buf = encoding.EncodeUint(buf, uint64(ns))
	}

	for i, v := range k.Values {
		// extract the sort order
		buf, err = encoding.EncodeValue(buf, v, order.IsDesc(i))
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
	if n == 0 {
		return nil, errors.Errorf("invalid key %v", key.Encoded)
	}
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
