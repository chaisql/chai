package types

import (
	"github.com/genjidb/genji/internal/errors"
)

var (
	// ErrValueNotFound must be returned by Array implementations, when calling the GetByIndex method and
	// the index wasn't found in the array.
	ErrValueNotFound = errors.New("value not found")
	errStop          = errors.New("stop")
)

// ValueType represents a value type supported by the database.
type ValueType uint8

// List of supported value types.
// These types are separated by family so that when
// new types are introduced we don't need to modify them.
const (
	// denote the absence of type
	AnyType ValueType = 0x0

	NullValue ValueType = 0x80

	BoolValue ValueType = 0x81

	// integer family: 0x90 to 0x9F
	IntegerValue ValueType = 0x90

	// double family: 0xA0 to 0xAF
	DoubleValue ValueType = 0xA0

	// string family: 0xC0 to 0xCF
	TextValue ValueType = 0xC0

	// blob family: 0xD0 to 0xDF
	BlobValue ValueType = 0xD0

	// array family: 0xE0 to 0xEF
	ArrayValue ValueType = 0xE0

	// document family: 0xF0 to 0xFF
	DocumentValue ValueType = 0xF0
)

func (t ValueType) String() string {
	switch t {
	case NullValue:
		return "null"
	case BoolValue:
		return "bool"
	case IntegerValue:
		return "integer"
	case DoubleValue:
		return "double"
	case BlobValue:
		return "blob"
	case TextValue:
		return "text"
	case ArrayValue:
		return "array"
	case DocumentValue:
		return "document"
	}

	return ""
}

// IsNumber returns true if t is either an integer of a float.
func (t ValueType) IsNumber() bool {
	return t == IntegerValue || t == DoubleValue
}

// IsAny returns whether this is type is Any or a real type
func (t ValueType) IsAny() bool {
	return t == AnyType
}

type Value interface {
	Type() ValueType
	V() interface{}
	String() string
	MarshalJSON() ([]byte, error)
	MarshalText() ([]byte, error)
}

// A Document represents a group of key value pairs.
type Document interface {
	// Iterate goes through all the fields of the document and calls the given function by passing each one of them.
	// If the given function returns an error, the iteration stops.
	Iterate(fn func(field string, value Value) error) error
	// GetByField returns a value by field name.
	// Must return ErrFieldNotFound if the field doesnt exist.
	GetByField(field string) (Value, error)
}

// An Array contains a set of values.
type Array interface {
	// Iterate goes through all the values of the array and calls the given function by passing each one of them.
	// If the given function returns an error, the iteration stops.
	Iterate(fn func(i int, value Value) error) error
	// GetByIndex returns a value by index of the array.
	GetByIndex(i int) (Value, error)
}
