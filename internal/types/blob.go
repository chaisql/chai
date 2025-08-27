package types

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"

	"github.com/chaisql/chai/internal/encoding"
	"github.com/cockroachdb/errors"
)

var _ TypeDefinition = BlobTypeDef{}

type BlobTypeDef struct{}

func (BlobTypeDef) New(v any) Value {
	return NewBlobValue(v.([]byte))
}

func (BlobTypeDef) Type() Type {
	return TypeBlob
}

func (BlobTypeDef) Decode(src []byte) (Value, int) {
	x, n := encoding.DecodeBlob(src)
	return NewBlobValue(x), n
}

func (BlobTypeDef) IsComparableWith(other Type) bool {
	return other == TypeBlob
}

func (BlobTypeDef) IsIndexComparableWith(other Type) bool {
	return other == TypeBlob
}

var _ Value = NewBlobValue(nil)

type BlobValue []byte

// NewBlobValue returns a SQL BLOB value.
func NewBlobValue(x []byte) BlobValue {
	return BlobValue(x)
}

func (v BlobValue) V() any {
	return []byte(v)
}

func (v BlobValue) Type() Type {
	return TypeBlob
}

func (v BlobValue) TypeDef() TypeDefinition {
	return BlobTypeDef{}
}

func (v BlobValue) IsZero() (bool, error) {
	return v == nil, nil
}

func (v BlobValue) String() string {
	t, _ := v.MarshalText()
	return string(t)
}

func (v BlobValue) MarshalText() ([]byte, error) {
	var dst bytes.Buffer
	dst.WriteString("\"\\x")
	_, _ = hex.NewEncoder(&dst).Write(v)
	dst.WriteByte('"')
	return dst.Bytes(), nil
}

func (v BlobValue) MarshalJSON() ([]byte, error) {
	dst := make([]byte, base64.StdEncoding.EncodedLen(len(v))+2)
	dst[0] = '"'
	dst[len(dst)-1] = '"'
	base64.StdEncoding.Encode(dst[1:], v)
	return dst, nil
}

func (v BlobValue) Encode(dst []byte) ([]byte, error) {
	return encoding.EncodeBlob(dst, v), nil
}

func (v BlobValue) EncodeAsKey(dst []byte) ([]byte, error) {
	return encoding.EncodeBlob(dst, v), nil
}

func (v BlobValue) CastAs(target Type) (Value, error) {
	switch target {
	case TypeBlob:
		return v, nil
	case TypeText:
		return NewTextValue(base64.StdEncoding.EncodeToString([]byte(v))), nil
	}

	return nil, errors.Errorf("cannot cast %s as %s", v.Type(), target)
}

func (v BlobValue) EQ(other Value) (bool, error) {
	if other.Type() != TypeBlob {
		return false, nil
	}

	return bytes.Equal([]byte(v), AsByteSlice(other)), nil
}

func (v BlobValue) GT(other Value) (bool, error) {
	t := other.Type()
	if t != TypeBlob {
		return false, nil
	}

	return bytes.Compare([]byte(v), AsByteSlice(other)) > 0, nil
}

func (v BlobValue) GTE(other Value) (bool, error) {
	t := other.Type()
	if t != TypeBlob {
		return false, nil
	}

	return bytes.Compare([]byte(v), AsByteSlice(other)) >= 0, nil
}

func (v BlobValue) LT(other Value) (bool, error) {
	t := other.Type()
	if t != TypeBlob {
		return false, nil
	}

	return bytes.Compare([]byte(v), AsByteSlice(other)) < 0, nil
}

func (v BlobValue) LTE(other Value) (bool, error) {
	t := other.Type()
	if t != TypeBlob {
		return false, nil
	}

	return bytes.Compare([]byte(v), AsByteSlice(other)) <= 0, nil
}

func (v BlobValue) Between(a, b Value) (bool, error) {
	if a.Type() != TypeBlob || b.Type() != TypeBlob {
		return false, nil
	}

	ok, err := a.LTE(v)
	if err != nil || !ok {
		return false, err
	}

	return b.GTE(v)
}
