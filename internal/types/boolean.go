package types

import "strconv"

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
