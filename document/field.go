package document

import (
	"fmt"
)

// A Field is a typed information stored in the database.
type Field struct {
	Value

	Name string

	// if the field is a nested record, the record is stored in this var
	nestedRecord Document
}

// NewField creates a field whose type is infered from x.
func NewField(name string, x interface{}) (Field, error) {
	v, err := New(x)
	if err != nil {
		return Field{}, err
	}

	return Field{Name: name, Value: v}, nil
}

// NewBytesField encodes x and returns a field.
func NewBytesField(name string, x []byte) Field {
	return Field{
		Name:  name,
		Value: NewBytes(x),
	}
}

// NewStringField encodes x and returns a field.
func NewStringField(name string, x string) Field {
	return Field{
		Name:  name,
		Value: NewString(x),
	}
}

// NewBoolField encodes x and returns a field.
func NewBoolField(name string, x bool) Field {
	return Field{
		Name:  name,
		Value: NewBool(x),
	}
}

// NewUintField encodes x and returns a field.
func NewUintField(name string, x uint) Field {
	return Field{
		Name:  name,
		Value: NewUint(x),
	}
}

// NewUint8Field encodes x and returns a field.
func NewUint8Field(name string, x uint8) Field {
	return Field{
		Name:  name,
		Value: NewUint8(x),
	}
}

// NewUint16Field encodes x and returns a field.
func NewUint16Field(name string, x uint16) Field {
	return Field{
		Name:  name,
		Value: NewUint16(x),
	}
}

// NewUint32Field encodes x and returns a field.
func NewUint32Field(name string, x uint32) Field {
	return Field{
		Name:  name,
		Value: NewUint32(x),
	}
}

// NewUint64Field encodes x and returns a field.
func NewUint64Field(name string, x uint64) Field {
	return Field{
		Name:  name,
		Value: NewUint64(x),
	}
}

// NewIntField encodes x and returns a field.
func NewIntField(name string, x int) Field {
	return Field{
		Name:  name,
		Value: NewInt(x),
	}
}

// NewInt8Field encodes x and returns a field.
func NewInt8Field(name string, x int8) Field {
	return Field{
		Name:  name,
		Value: NewInt8(x),
	}
}

// NewInt16Field encodes x and returns a field.
func NewInt16Field(name string, x int16) Field {
	return Field{
		Name:  name,
		Value: NewInt16(x),
	}
}

// NewInt32Field encodes x and returns a field.
func NewInt32Field(name string, x int32) Field {
	return Field{
		Name:  name,
		Value: NewInt32(x),
	}
}

// NewInt64Field encodes x and returns a field.
func NewInt64Field(name string, x int64) Field {
	return Field{
		Name:  name,
		Value: NewInt64(x),
	}
}

// NewFloat64Field encodes x and returns a field.
func NewFloat64Field(name string, x float64) Field {
	return Field{
		Name:  name,
		Value: NewFloat64(x),
	}
}

// NewNullField returns a null field.
func NewNullField(name string) Field {
	return Field{
		Name:  name,
		Value: NewNull(),
	}
}

func NewObjectField(name string, r Document) Field {
	return Field{
		Name: name,
		Value: Value{
			Type: Object,
		},
		nestedRecord: r,
	}
}

func (f Field) String() string {
	return fmt.Sprintf("%s:%s", f.Name, f.Value)
}

func (f *Field) Decode() (interface{}, error) {
	if f.Type == Object {
		return f.nestedRecord, nil
	}

	return f.Value.Decode()
}
