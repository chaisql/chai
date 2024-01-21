package types

import "github.com/cockroachdb/errors"

var _ Value = NewArrayValue(nil)

type ArrayValue struct {
	a Array
}

// NewArrayValue returns a SQL ARRAY value.
func NewArrayValue(x Array) *ArrayValue {
	return &ArrayValue{
		a: x,
	}
}

func (v *ArrayValue) V() any {
	return v.a
}

func (v *ArrayValue) Type() ValueType {
	return TypeArray
}

func (v *ArrayValue) IsZero() (bool, error) {
	// The zero value of an array is an empty array.
	// Thus, if GetByIndex(0) returns the ErrValueNotFound
	// it means that the array is empty.
	_, err := v.a.GetByIndex(0)
	if errors.Is(err, ErrValueNotFound) {
		return true, nil
	}
	return false, err
}

func (v *ArrayValue) String() string {
	data, _ := v.MarshalText()
	return string(data)
}

func (v *ArrayValue) MarshalText() ([]byte, error) {
	return MarshalTextIndent(v, "", "")
}

func (v *ArrayValue) MarshalJSON() ([]byte, error) {
	return jsonArray{Array: v.a}.MarshalJSON()
}

func (v *ArrayValue) EQ(other Value) (bool, error) {
	t := other.Type()
	if t != TypeArray {
		return false, nil
	}

	return compareArrays(operatorEq, v.a, AsArray(other))
}

func (v *ArrayValue) GT(other Value) (bool, error) {
	t := other.Type()
	if t != TypeArray {
		return false, nil
	}

	return compareArrays(operatorGt, v.a, AsArray(other))
}

func (v *ArrayValue) GTE(other Value) (bool, error) {
	t := other.Type()
	if t != TypeArray {
		return false, nil
	}

	return compareArrays(operatorGte, v.a, AsArray(other))
}

func (v *ArrayValue) LT(other Value) (bool, error) {
	t := other.Type()
	if t != TypeArray {
		return false, nil
	}

	return compareArrays(operatorLt, v.a, AsArray(other))
}

func (v *ArrayValue) LTE(other Value) (bool, error) {
	t := other.Type()
	if t != TypeArray {
		return false, nil
	}

	return compareArrays(operatorLte, v.a, AsArray(other))
}

func (v *ArrayValue) Between(a, b Value) (bool, error) {
	if a.Type() != TypeArray || b.Type() != TypeArray {
		return false, nil
	}

	ok, err := a.LTE(v)
	if err != nil || !ok {
		return false, err
	}

	return b.GTE(v)
}
