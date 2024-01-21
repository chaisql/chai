package types

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
)

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
