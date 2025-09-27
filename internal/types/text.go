package types

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"github.com/chaisql/chai/internal/encoding"
	"github.com/cockroachdb/errors"
)

var _ TypeDefinition = TextTypeDef{}

type TextTypeDef struct{}

func (TextTypeDef) Decode(src []byte) (Value, int) {
	x, n := encoding.DecodeText(src)
	return NewTextValue(x), n
}

func (TextTypeDef) IsComparableWith(other Type) bool {
	return other == TypeNull || other == TypeText || other == TypeBoolean || other == TypeInteger || other == TypeBigint || other == TypeDoublePrecision || other == TypeTimestamp || other == TypeBytea
}

func (t TextTypeDef) IsIndexComparableWith(other Type) bool {
	return t.IsComparableWith(other)
}

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

func (v TextValue) TypeDef() TypeDefinition {
	return TextTypeDef{}
}

func (v TextValue) IsZero() (bool, error) {
	return v == "", nil
}

func (v TextValue) String() string {
	// Use single quotes for SQL string literal representation.
	// strconv.Quote always uses double quotes, so build the quoted
	// representation manually, escaping single quotes inside the text.
	s := string(v)
	s = strings.ReplaceAll(s, "'", "\\'")
	return "'" + s + "'"
}

func (v TextValue) MarshalText() ([]byte, error) {
	return []byte(v.String()), nil
}

func (v TextValue) MarshalJSON() ([]byte, error) {
	// JSON must use double quotes. Use strconv.Quote to produce a valid
	// JSON string regardless of the SQL text literal representation.
	return []byte(strconv.Quote(string(v))), nil
}

func (v TextValue) Encode(dst []byte) ([]byte, error) {
	return encoding.EncodeText(dst, string(v)), nil
}

func (v TextValue) EncodeAsKey(dst []byte) ([]byte, error) {
	return v.Encode(dst)
}

func (v TextValue) CastAs(target Type) (Value, error) {
	switch target {
	case TypeText:
		return v, nil
	case TypeBoolean:
		switch strings.ToLower(string(v)) {
		case "1", "t", "true", "y", "yes", "on":
			return NewBooleanValue(true), nil
		case "0", "f", "false", "n", "no", "off":
			return NewBooleanValue(false), nil
		}

		return nil, fmt.Errorf("cannot cast %q as boolean", v.V())
	case TypeInteger:
		i, err := strconv.ParseInt(string(v), 10, 32)
		if err != nil {
			intErr := err
			f, err := strconv.ParseFloat(string(v), 64)
			if err != nil {
				return nil, errors.Errorf(`cannot cast %q as integer: %w`, v.V(), intErr)
			}
			i = int64(f)
		}
		return NewIntegerValue(int32(i)), nil
	case TypeBigint:
		i, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			intErr := err
			f, err := strconv.ParseFloat(string(v), 64)
			if err != nil {
				return nil, fmt.Errorf(`cannot cast %q as bigint: %w`, v.V(), intErr)
			}
			i = int64(f)
		}
		return NewBigintValue(i), nil
	case TypeDoublePrecision:
		f, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			return nil, fmt.Errorf(`cannot cast %q as double: %w`, v.V(), err)
		}
		return NewDoublePrecisionValue(f), nil
	case TypeTimestamp:
		t, err := ParseTimestamp(string(v))
		if err != nil {
			return nil, fmt.Errorf(`cannot cast %q as timestamp: %w`, v.V(), err)
		}
		return NewTimestampValue(t), nil
	case TypeBytea:
		s := string(v)
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, err
		}

		return NewByteaValue(b), nil
	}

	return nil, errors.Errorf("cannot cast %q as %q", v.Type(), target)
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
