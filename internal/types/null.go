package types

var _ Value = NewNullValue()

type NullValue struct{}

// NewNullValue returns a SQL BOOLEAN value.
func NewNullValue() NullValue {
	return NullValue{}
}

func (v NullValue) V() any {
	return nil
}

func (v NullValue) Type() ValueType {
	return TypeNull
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
