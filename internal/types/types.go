package types

import (
	"fmt"

	"github.com/chaisql/chai/internal/encoding"
	"github.com/cockroachdb/errors"
)

var (
	// ErrColumnNotFound must be returned by row implementations, when calling the Get method and
	// the column doesn't exist.
	ErrColumnNotFound = errors.New("column not found")
)

// Type represents a type supported by the database.
type Type uint8

// List of supported types.
const (
	// TypeAny denotes the absence of type
	TypeAny Type = iota
	TypeNull
	TypeBoolean
	TypeInteger
	TypeBigint
	TypeDouble
	TypeTimestamp
	TypeText
	TypeBytea
)

func (t Type) Def() TypeDefinition {
	switch t {
	case TypeNull:
		return NullTypeDef{}
	case TypeBoolean:
		return BooleanTypeDef{}
	case TypeInteger:
		return IntegerTypeDef{}
	case TypeBigint:
		return BigintTypeDef{}
	case TypeDouble:
		return DoubleTypeDef{}
	case TypeTimestamp:
		return TimestampTypeDef{}
	case TypeText:
		return TextTypeDef{}
	case TypeBytea:
		return ByteaTypeDef{}
	}

	return nil
}

func (t Type) String() string {
	switch t {
	case TypeNull:
		return "null"
	case TypeBoolean:
		return "boolean"
	case TypeInteger:
		return "integer"
	case TypeBigint:
		return "bigint"
	case TypeDouble:
		return "double"
	case TypeTimestamp:
		return "timestamp"
	case TypeBytea:
		return "bytea"
	case TypeText:
		return "text"
	}

	panic(fmt.Sprintf("unsupported type %#v", t))
}

func (t Type) MinEnctype() byte {
	switch t {
	case TypeNull:
		return encoding.NullValue
	case TypeBoolean:
		return encoding.FalseValue
	case TypeInteger:
		return encoding.Int32Value
	case TypeBigint:
		return encoding.Int64Value
	case TypeDouble:
		return encoding.Float64Value
	case TypeTimestamp:
		return encoding.Int64Value
	case TypeText:
		return encoding.TextValue
	case TypeBytea:
		return encoding.ByteaValue
	default:
		panic(fmt.Sprintf("unsupported type %v", t))
	}
}

func (t Type) MinEnctypeDesc() byte {
	switch t {
	case TypeNull:
		return encoding.DESC_NullValue
	case TypeBoolean:
		return encoding.DESC_TrueValue
	case TypeInteger:
		return encoding.DESC_Uint32Value
	case TypeBigint:
		return encoding.DESC_Uint64Value
	case TypeDouble:
		return encoding.DESC_Float64Value
	case TypeTimestamp:
		return encoding.DESC_Uint64Value
	case TypeText:
		return encoding.DESC_TextValue
	case TypeBytea:
		return encoding.DESC_ByteaValue
	default:
		panic(fmt.Sprintf("unsupported type %v", t))
	}
}

func (t Type) MaxEnctype() byte {
	switch t {
	case TypeNull:
		return encoding.NullValue + 1
	case TypeBoolean:
		return encoding.TrueValue + 1
	case TypeInteger:
		return encoding.Uint32Value + 1
	case TypeBigint:
		return encoding.Uint64Value + 1
	case TypeDouble:
		return encoding.Float64Value + 1
	case TypeTimestamp:
		return encoding.Uint64Value + 1
	case TypeText:
		return encoding.TextValue + 1
	case TypeBytea:
		return encoding.ByteaValue + 1
	default:
		panic(fmt.Sprintf("unsupported type %v", t))
	}
}

func (t Type) MaxEnctypeDesc() byte {
	switch t {
	case TypeNull:
		return encoding.DESC_NullValue + 1
	case TypeBoolean:
		return encoding.DESC_FalseValue + 1
	case TypeInteger:
		return encoding.DESC_Int64Value + 1
	case TypeDouble:
		return encoding.DESC_Float64Value + 1
	case TypeTimestamp:
		return encoding.DESC_Int64Value + 1
	case TypeText:
		return encoding.DESC_TextValue + 1
	case TypeBytea:
		return encoding.DESC_ByteaValue + 1
	default:
		panic(fmt.Sprintf("unsupported type %v", t))
	}
}

// IsNumber returns true if t is either an integer or a float.
func (t Type) IsNumber() bool {
	return t == TypeInteger || t == TypeBigint || t == TypeDouble
}

func (t Type) IsInteger() bool {
	return t == TypeInteger || t == TypeBigint
}

// IsTimestampCompatible returns true if t is either a timestamp, an integer, or a text.
func (t Type) IsTimestampCompatible() bool {
	return t == TypeTimestamp || t == TypeText
}

func (t Type) IsComparableWith(other Type) bool {
	if t == other {
		return true
	}

	if t.IsNumber() && other.IsNumber() {
		return true
	}

	if t.IsTimestampCompatible() && other.IsTimestampCompatible() {
		return true
	}

	return false
}

// IsAny returns whether this is type is Any or a real type
func (t Type) IsAny() bool {
	return t == TypeAny
}

type TypeDefinition interface {
	New(v any) Value
	Type() Type
	Decode(src []byte) (Value, int)
	IsComparableWith(other Type) bool
	IsIndexComparableWith(other Type) bool
}

type Comparable interface {
	EQ(other Value) (bool, error)
	GT(other Value) (bool, error)
	GTE(other Value) (bool, error)
	LT(other Value) (bool, error)
	LTE(other Value) (bool, error)
	Between(a, b Value) (bool, error)
}
