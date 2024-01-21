package types

import (
	"strconv"
	"strings"
)

var _ Value = NewTextValue("")

type TextValue string

// NewTextValue returns a SQL TEXT value.
func NewTextValue(x string) TextValue {
	return TextValue(x)
}

func (v TextValue) V() any {
	return string(v)
}

func (v TextValue) Type() Type {
	return TypeText
}

func (v TextValue) IsZero() (bool, error) {
	return v == "", nil
}

func (v TextValue) String() string {
	return strconv.Quote(string(v))
}

func (v TextValue) MarshalText() ([]byte, error) {
	return []byte(v.String()), nil
}

func (v TextValue) MarshalJSON() ([]byte, error) {
	return v.MarshalText()
}

func (v TextValue) EQ(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeText:
		return strings.Compare(string(v), AsString(other)) == 0, nil
	case TypeTimestamp:
		ts, err := ParseTimestamp(AsString(v))
		if err != nil {
			return false, err
		}
		return ts.Equal(AsTime(other)), nil
	default:
		return false, nil
	}
}

func (v TextValue) GT(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeText:
		return strings.Compare(string(v), AsString(other)) > 0, nil
	case TypeTimestamp:
		ts, err := ParseTimestamp(AsString(v))
		if err != nil {
			return false, err
		}
		return ts.After(AsTime(other)), nil
	default:
		return false, nil
	}
}

func (v TextValue) GTE(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeText:
		return strings.Compare(string(v), AsString(other)) >= 0, nil
	case TypeTimestamp:
		t1, err := ParseTimestamp(AsString(v))
		if err != nil {
			return false, err
		}
		t2 := AsTime(other)
		return t1.After(t2) || t1.Equal(t2), nil
	default:
		return false, nil
	}
}

func (v TextValue) LT(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeText:
		return strings.Compare(string(v), AsString(other)) < 0, nil
	case TypeTimestamp:
		ts, err := ParseTimestamp(AsString(v))
		if err != nil {
			return false, err
		}
		return ts.Before(AsTime(other)), nil
	default:
		return false, nil
	}
}

func (v TextValue) LTE(other Value) (bool, error) {
	t := other.Type()
	switch t {
	case TypeText:
		return strings.Compare(string(v), AsString(other)) <= 0, nil
	case TypeTimestamp:
		t1, err := ParseTimestamp(AsString(v))
		if err != nil {
			return false, err
		}
		t2 := AsTime(other)
		return t1.Before(t2) || t1.Equal(t2), nil
	default:
		return false, nil
	}
}

func (v TextValue) Between(a, b Value) (bool, error) {
	if a.Type() != TypeText || b.Type() != TypeText {
		return false, nil
	}

	ok, err := a.LTE(v)
	if err != nil || !ok {
		return false, err
	}

	return b.GTE(v)
}
