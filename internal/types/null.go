package types

import (
	"github.com/chaisql/chai/internal/encoding"
	"github.com/cockroachdb/errors"
)

var _ TypeDefinition = NullTypeDef{}

type NullTypeDef struct{}

func (NullTypeDef) Decode(src []byte) (Value, int) {
	if src[0] != encoding.NullValue && src[0] != encoding.DESC_NullValue {
		panic(errors.New("invalid encoded null value"))
	}

	return NewNullValue(), 1
}

func (NullTypeDef) IsComparableWith(other Type) bool {
	return other == TypeNull
}

func (NullTypeDef) IsIndexComparableWith(other Type) bool {
	return other == TypeNull
}

var _ Value = NewNullValue()

type NullValue struct{}

// NewNullValue returns a SQL NULL value.
func NewNullValue() NullValue {
	return NullValue{}
}

func (v NullValue) V() any {
	return nil
}

func (v NullValue) Type() Type {
	return TypeNull
}

func (v NullValue) TypeDef() TypeDefinition {
	return NullTypeDef{}
}

func (v NullValue) IsZero() (bool, error) {
	return false, nil
}

func (v NullValue) String() string {
	return "NULL"
}

func (v NullValue) MarshalText() ([]byte, error) {
	return []byte("NULL"), nil
}

func (v NullValue) MarshalJSON() ([]byte, error) {
	return []byte("null"), nil
}

func (v NullValue) Encode(dst []byte) ([]byte, error) {
	return encoding.EncodeNull(dst), nil
}

func (v NullValue) EncodeAsKey(dst []byte) ([]byte, error) {
	return v.Encode(dst)
}

func (v NullValue) CastAs(target Type) (Value, error) {
	return v, nil
}

func (v NullValue) EQ(other Value) (bool, error) {
	return other.Type() == TypeNull, nil
}

func (v NullValue) GT(other Value) (bool, error) {
	return false, nil
}

func (v NullValue) GTE(other Value) (bool, error) {
	return other.Type() == TypeNull, nil
}

func (v NullValue) LT(other Value) (bool, error) {
	return false, nil
}

func (v NullValue) LTE(other Value) (bool, error) {
	return other.Type() == TypeNull, nil
}

func (v NullValue) Between(a, b Value) (bool, error) {
	return false, nil
}
