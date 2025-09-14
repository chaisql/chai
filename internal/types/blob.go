package types

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"

	"github.com/chaisql/chai/internal/encoding"
	"github.com/cockroachdb/errors"
)

var _ TypeDefinition = ByteaTypeDef{}

type ByteaTypeDef struct{}

func (ByteaTypeDef) Decode(src []byte) (Value, int) {
	x, n := encoding.DecodeBytea(src)
	return NewByteaValue(x), n
}

func (ByteaTypeDef) IsComparableWith(other Type) bool {
	return other == TypeBytea
}

func (ByteaTypeDef) IsIndexComparableWith(other Type) bool {
	return other == TypeBytea
}

var _ Value = NewByteaValue(nil)

type ByteaValue []byte

// NewByteaValue returns a SQL BYTEA value.
func NewByteaValue(x []byte) ByteaValue {
	return ByteaValue(x)
}

func (v ByteaValue) V() any {
	return []byte(v)
}

func (v ByteaValue) Type() Type {
	return TypeBytea
}

func (v ByteaValue) TypeDef() TypeDefinition {
	return ByteaTypeDef{}
}

func (v ByteaValue) IsZero() (bool, error) {
	return v == nil, nil
}

func (v ByteaValue) String() string {
	t, _ := v.MarshalText()
	return string(t)
}

func (v ByteaValue) MarshalText() ([]byte, error) {
	var dst bytes.Buffer
	dst.WriteString("\"\\x")
	_, _ = hex.NewEncoder(&dst).Write(v)
	dst.WriteByte('"')
	return dst.Bytes(), nil
}

func (v ByteaValue) MarshalJSON() ([]byte, error) {
	dst := make([]byte, base64.StdEncoding.EncodedLen(len(v))+2)
	dst[0] = '"'
	dst[len(dst)-1] = '"'
	base64.StdEncoding.Encode(dst[1:], v)
	return dst, nil
}

func (v ByteaValue) Encode(dst []byte) ([]byte, error) {
	return encoding.EncodeBytea(dst, v), nil
}

func (v ByteaValue) EncodeAsKey(dst []byte) ([]byte, error) {
	return encoding.EncodeBytea(dst, v), nil
}

func (v ByteaValue) CastAs(target Type) (Value, error) {
	switch target {
	case TypeBytea:
		return v, nil
	case TypeText:
		return NewTextValue(base64.StdEncoding.EncodeToString([]byte(v))), nil
	}

	return nil, errors.Errorf("cannot cast %s as %s", v.Type(), target)
}

func (v ByteaValue) EQ(other Value) (bool, error) {
	if other.Type() != TypeBytea {
		return false, nil
	}

	return bytes.Equal([]byte(v), AsByteSlice(other)), nil
}

func (v ByteaValue) GT(other Value) (bool, error) {
	t := other.Type()
	if t != TypeBytea {
		return false, nil
	}

	return bytes.Compare([]byte(v), AsByteSlice(other)) > 0, nil
}

func (v ByteaValue) GTE(other Value) (bool, error) {
	t := other.Type()
	if t != TypeBytea {
		return false, nil
	}

	return bytes.Compare([]byte(v), AsByteSlice(other)) >= 0, nil
}

func (v ByteaValue) LT(other Value) (bool, error) {
	t := other.Type()
	if t != TypeBytea {
		return false, nil
	}

	return bytes.Compare([]byte(v), AsByteSlice(other)) < 0, nil
}

func (v ByteaValue) LTE(other Value) (bool, error) {
	t := other.Type()
	if t != TypeBytea {
		return false, nil
	}

	return bytes.Compare([]byte(v), AsByteSlice(other)) <= 0, nil
}

func (v ByteaValue) Between(a, b Value) (bool, error) {
	if a.Type() != TypeBytea || b.Type() != TypeBytea {
		return false, nil
	}

	ok, err := a.LTE(v)
	if err != nil || !ok {
		return false, err
	}

	return b.GTE(v)
}
