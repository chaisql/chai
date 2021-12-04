package tree

import (
	"bytes"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/types"
	"github.com/genjidb/genji/types/encoding"
)

type Key []byte

func NewKey(values ...types.Value) (Key, error) {
	if len(values) == 0 {
		return nil, nil
	}

	if values[0].Type() != types.NullValue && values[0].V() == nil {
		return nil, errors.New("cannot encode nil value")
	}

	var key Key
	var buf bytes.Buffer
	err := encoding.NewValueEncoder(&buf).
		Encode(types.NewArrayValue(document.NewValueBuffer(values...)))
	if err != nil {
		return nil, err
	}

	key = buf.Bytes()
	// remove '[' and ']'
	key = key[1 : len(key)-1]
	return key, nil
}

func NewMinKeyForType(t types.ValueType) Key {
	return []byte{byte(t)}
}

func NewMaxKeyForType(t types.ValueType) Key {
	return []byte{byte(t + 1)}
}

func (k Key) String() string {
	values, _ := k.Decode()

	return types.NewArrayValue(document.NewValueBuffer(values...)).String()
}

func (key Key) Decode() ([]types.Value, error) {
	var buf bytes.Buffer

	buf.Grow(len(key) + 2)
	buf.WriteByte(byte(types.ArrayValue))
	buf.Write(key)
	buf.WriteByte(encoding.ArrayEnd)
	kv, err := encoding.DecodeValue(buf.Bytes())
	if err != nil {
		return nil, err
	}

	vb := kv.V().(*document.ValueBuffer)

	return vb.Values, nil
}

type Keys []Key

func (a Keys) Len() int      { return len(a) }
func (a Keys) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a Keys) Less(i, j int) bool {
	return bytes.Compare(a[i], a[j]) < 0
}
