package tree

import (
	"strings"

	"github.com/chaisql/chai/internal/encoding"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

type Key struct {
	values  []types.Value
	Encoded []byte
}

func NewKey(values ...types.Value) *Key {
	return &Key{
		values: values,
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

	for i, v := range k.values {
		// extract the sort order
		buf, err = types.EncodeValueAsKey(buf, v, order.IsDesc(i))
		if err != nil {
			return nil, err
		}
	}

	k.Encoded = buf
	return buf, nil
}

func (key *Key) Decode() ([]types.Value, error) {
	if len(key.values) > 0 {
		return key.values, nil
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
		v, n := types.DecodeValue(b)
		b = b[n:]

		values = append(values, v)
		if len(b) == 0 {
			break
		}
	}

	return values, nil
}

func (k *Key) String() string {
	if k == nil {
		return ""
	}
	values, _ := k.Decode()

	var sb strings.Builder
	sb.WriteString("(")
	for i, v := range values {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(v.String())
	}
	sb.WriteString(")")
	return sb.String()
}
