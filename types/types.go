package types

import (
	"github.com/cockroachdb/errors"
)

var (
	// ErrFieldNotFound must be returned by Document implementations, when calling the GetByField method and
	// the field wasn't found in the document.
	ErrFieldNotFound = errors.New("field not found")
	// ErrValueNotFound must be returned by Array implementations, when calling the GetByIndex method and
	// the index wasn't found in the array.
	ErrValueNotFound = errors.New("value not found")

	errStop = errors.New("stop")
)

// ValueType represents a value type supported by the database.
type ValueType uint8

// List of supported value types.
const (
	// denote the absence of type
	AnyValue ValueType = 0x00

	NullValue ValueType = 0x05

	BooleanValue ValueType = 0x10

	IntegerValue ValueType = 0x20

	DoubleValue ValueType = 0xD0

	TextValue ValueType = 0xDA

	BlobValue ValueType = 0xE0

	ArrayValue ValueType = 0xE6

	DocumentValue ValueType = 0xF0
)

func (t ValueType) String() string {
	switch t {
	case NullValue:
		return "null"
	case BooleanValue:
		return "boolean"
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

	return "any"
}

// IsNumber returns true if t is either an integer of a float.
func (t ValueType) IsNumber() bool {
	return t == IntegerValue || t == DoubleValue
}

// IsAny returns whether this is type is Any or a real type
func (t ValueType) IsAny() bool {
	return t == AnyValue
}

type Value interface {
	Type() ValueType
	V() any
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

	// MarshalJSON implements the json.Marshaler interface.
	// It returns a JSON representation of the document.
	MarshalJSON() ([]byte, error)
}

// An Array contains a set of values.
type Array interface {
	// Iterate goes through all the values of the array and calls the given function by passing each one of them.
	// If the given function returns an error, the iteration stops.
	Iterate(fn func(i int, value Value) error) error
	// GetByIndex returns a value by index of the array.
	GetByIndex(i int) (Value, error)

	// MarshalJSON implements the json.Marshaler interface.
	// It returns a JSON representation of the array.
	MarshalJSON() ([]byte, error)
}
