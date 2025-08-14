package types

import (
	"strconv"

	"github.com/chaisql/chai/internal/encoding"
	"github.com/cockroachdb/errors"
)

var _ TypeDefinition = BooleanTypeDef{}

type BooleanTypeDef struct{}

func (BooleanTypeDef) New(v any) Value {
	return NewBooleanValue(v.(bool))
}

func (BooleanTypeDef) Type() Type {
	return TypeBoolean
}

func (t BooleanTypeDef) Decode(src []byte) (Value, int) {
	b := encoding.DecodeBoolean(src)
	return NewBooleanValue(b), 1
}

func (BooleanTypeDef) IsComparableWith(other Type) bool {
	return other == TypeBoolean
}

func (BooleanTypeDef) IsIndexComparableWith(other Type) bool {
	return other == TypeBoolean
}

var _ Value = NewBooleanValue(false)

type BooleanValue bool

// NewBooleanValue returns a SQL BOOLEAN value.
func NewBooleanValue(x bool) BooleanValue {
	return BooleanValue(x)
}

func (v BooleanValue) V() any {
	return bool(v)
}

func (v BooleanValue) Type() Type {
	return TypeBoolean
}

func (v BooleanValue) TypeDef() TypeDefinition {
	return BooleanTypeDef{}
}

func (v BooleanValue) IsZero() (bool, error) {
	return !bool(v), nil
}

func (v BooleanValue) String() string {
	return strconv.FormatBool(bool(v))
}

func (v BooleanValue) MarshalText() ([]byte, error) {
	return []byte(strconv.FormatBool(bool(v))), nil
}

func (v BooleanValue) MarshalJSON() ([]byte, error) {
	return v.MarshalText()
}

func (v BooleanValue) Encode(dst []byte) ([]byte, error) {
	return encoding.EncodeBoolean(dst, bool(v)), nil
}

func (v BooleanValue) EncodeAsKey(dst []byte) ([]byte, error) {
	return v.Encode(dst)
}

func (v BooleanValue) CastAs(target Type) (Value, error) {
	switch target {
	case TypeBoolean:
		return v, nil
	case TypeInteger:
		if bool(v) {
			return NewIntegerValue(1), nil
		}

		return NewIntegerValue(0), nil
	case TypeText:
		return NewTextValue(v.String()), nil
	}

	return nil, errors.Errorf("cannot cast %s as %s", v.Type(), target)
}

func (v BooleanValue) ConvertToIndexedType(t Type) (Value, error) {
	return v, nil
}

func (v BooleanValue) EQ(other Value) (bool, error) {
	if other.Type() != TypeBoolean {
		return false, nil
	}

	return bool(v) == AsBool(other), nil
}

func (v BooleanValue) GT(other Value) (bool, error) {
	if other.Type() != TypeBoolean {
		return false, nil
	}

	return bool(v) && !AsBool(other), nil
}

func (v BooleanValue) GTE(other Value) (bool, error) {
	if other.Type() != TypeBoolean {
		return false, nil
	}

	bv := bool(v)
	return bv == AsBool(other) || bv, nil
}

func (v BooleanValue) LT(other Value) (bool, error) {
	if other.Type() != TypeBoolean {
		return false, nil
	}

	return !bool(v) && AsBool(other), nil
}

func (v BooleanValue) LTE(other Value) (bool, error) {
	if other.Type() != TypeBoolean {
		return false, nil
	}

	bv := bool(v)
	return bv == AsBool(other) || !bv, nil
}

func (v BooleanValue) Between(a, b Value) (bool, error) {
	if a.Type() != TypeBoolean || b.Type() != TypeBoolean {
		return false, nil
	}

	ok, err := a.LTE(v)
	if err != nil || !ok {
		return false, err
	}

	return b.GTE(v)
}
