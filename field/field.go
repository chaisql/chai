// Package field defines types to manipulate and encode fields.
package field

import (
	"fmt"

	"github.com/asdine/genji/value"
)

// A Field is a typed information stored in the database.
type Field struct {
	value.Value

	Name string
}

// New creates a field whose type is infered from x.
func New(name string, x interface{}) (Field, error) {
	v, err := value.New(x)
	if err != nil {
		return Field{}, err
	}

	return Field{Name: name, Value: v}, nil
}

// NewBytes encodes x and returns a field.
func NewBytes(name string, x []byte) Field {
	return Field{
		Name:  name,
		Value: value.NewBytes(x),
	}
}

// NewString encodes x and returns a field.
func NewString(name string, x string) Field {
	return Field{
		Name:  name,
		Value: value.NewString(x),
	}
}

// NewBool encodes x and returns a field.
func NewBool(name string, x bool) Field {
	return Field{
		Name:  name,
		Value: value.NewBool(x),
	}
}

// NewUint encodes x and returns a field.
func NewUint(name string, x uint) Field {
	return Field{
		Name:  name,
		Value: value.NewUint(x),
	}
}

// NewUint8 encodes x and returns a field.
func NewUint8(name string, x uint8) Field {
	return Field{
		Name:  name,
		Value: value.NewUint8(x),
	}
}

// NewUint16 encodes x and returns a field.
func NewUint16(name string, x uint16) Field {
	return Field{
		Name:  name,
		Value: value.NewUint16(x),
	}
}

// NewUint32 encodes x and returns a field.
func NewUint32(name string, x uint32) Field {
	return Field{
		Name:  name,
		Value: value.NewUint32(x),
	}
}

// NewUint64 encodes x and returns a field.
func NewUint64(name string, x uint64) Field {
	return Field{
		Name:  name,
		Value: value.NewUint64(x),
	}
}

// NewInt encodes x and returns a field.
func NewInt(name string, x int) Field {
	return Field{
		Name:  name,
		Value: value.NewInt(x),
	}
}

// NewInt8 encodes x and returns a field.
func NewInt8(name string, x int8) Field {
	return Field{
		Name:  name,
		Value: value.NewInt8(x),
	}
}

// NewInt16 encodes x and returns a field.
func NewInt16(name string, x int16) Field {
	return Field{
		Name:  name,
		Value: value.NewInt16(x),
	}
}

// NewInt32 encodes x and returns a field.
func NewInt32(name string, x int32) Field {
	return Field{
		Name:  name,
		Value: value.NewInt32(x),
	}
}

// NewInt64 encodes x and returns a field.
func NewInt64(name string, x int64) Field {
	return Field{
		Name:  name,
		Value: value.NewInt64(x),
	}
}

// NewFloat32 encodes x and returns a field.
func NewFloat32(name string, x float32) Field {
	return Field{
		Name:  name,
		Value: value.NewFloat32(x),
	}
}

// NewFloat64 encodes x and returns a field.
func NewFloat64(name string, x float64) Field {
	return Field{
		Name:  name,
		Value: value.NewFloat64(x),
	}
}

func (f Field) String() string {
	return fmt.Sprintf("%s:%s", f.Name, f.Value)
}
