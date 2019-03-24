package field

import (
	"encoding/binary"
	"errors"
)

// Type represents a field type supported by the database.
type Type uint8

// List of supported field types.
const (
	String Type = iota + 1
	Bytes
	Uint
	Uint8
	Uint16
	Uint32
	Uint64
	Int
	Int8
	Int16
	Int32
	Int64
	Float32
	Float64
	Bool

	Slice = iota + 100
	Map
)

// A Field is a typed information stored in the database.
type Field struct {
	Name string
	Type Type
	Data []byte
}

func NewString(name, value string) Field {
	return Field{
		Name: name,
		Type: String,
		Data: []byte(value),
	}
}

func NewInt64(name string, value int64) Field {
	return Field{
		Name: name,
		Type: Int64,
		Data: EncodeInt64(value),
	}
}

// EncodeInt64 takes an int64 and returns its binary representation.
// The size of the returned buffer depends on the size of the number.
func EncodeInt64(i int64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, i)
	return buf[:n]
}

// DecodeInt64 takes a byte slice and decodes it into an int64.
// An error is returned if the value is invalid.
func DecodeInt64(v []byte) (int64, error) {
	i, n := binary.Varint(v)
	if n <= 0 {
		return 0, errors.New("field: cannot decode value to in64")
	}

	return i, nil
}
