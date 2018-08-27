package record

import (
	"errors"

	"github.com/asdine/genji/field"
)

// A Record holds a group of fields.
type Record interface {
	Field(string) (*field.Field, error)
	Cursor() Cursor
}

// A Cursor iterates over the fields of a record.
type Cursor interface {
	// Next advances the cursor to the next field which will then be available
	// through the Field method. It returns false when the cursor stops.
	// If an error occurs during iteration, the Err method will return it.
	Next() bool

	// Err returns the error, if any, that was encountered during iteration.
	Err() error

	// Field returns the current field.
	Field() (*field.Field, error)
}

// FieldBuffer contains a list of fields. It implements the Record interface.
type FieldBuffer []*field.Field

func (fb *FieldBuffer) Add(f *field.Field) {
	*fb = append(*fb, f)
}

func (fb FieldBuffer) Bytes(name string) ([]byte, error) {
	f, err := fb.Field(name)
	if err != nil {
		return nil, err
	}

	return f.Data, nil
}

func (fb FieldBuffer) Field(name string) (*field.Field, error) {
	for _, f := range fb {
		if f.Name == name {
			return f, nil
		}
	}

	return nil, errors.New("not found")
}

// Cursor creates a Cursor that iterate over the slice of fields.
func (fb FieldBuffer) Cursor() Cursor {
	return &fieldBufferCursor{buf: fb, i: -1}
}

type fieldBufferCursor struct {
	i   int
	buf FieldBuffer
}

func (c *fieldBufferCursor) Next() bool {
	if c.i+1 >= len(c.buf) {
		return false
	}

	c.i++
	return true
}

func (c *fieldBufferCursor) Field() (*field.Field, error) {
	return c.buf[c.i], nil
}

func (c *fieldBufferCursor) Err() error {
	return nil
}
