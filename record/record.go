// Package record defines interfaces, implementations and helpers to manipulate and encode records.
package record

import (
	"fmt"
)

// A Record represents a group of fields.
type Record interface {
	// Iterate goes through all the fields of the record and calls the given function by passing each one of them.
	// If the given function returns an error, the iteration stops.
	Iterate(fn func(Field) error) error
	// GetField returns a field by name.
	GetField(name string) (Field, error)
}

// A Keyer returns the key identifying records in their storage.
// This is usually implemented by records read from storages.
type Keyer interface {
	Key() []byte
}

// A Scanner can iterate over a record and scan all the fields.
type Scanner interface {
	ScanRecord(Record) error
}

// FieldBuffer is slice of fields which implements the Record interface.
type FieldBuffer []Field

// NewFieldBuffer creates a FieldBuffer with the given fields.
func NewFieldBuffer(fields ...Field) FieldBuffer {
	return FieldBuffer(fields)
}

// Add a field to the buffer.
func (fb *FieldBuffer) Add(f Field) {
	*fb = append(*fb, f)
}

// ScanRecord copies all the fields of r to the buffer.
func (fb *FieldBuffer) ScanRecord(r Record) error {
	return r.Iterate(func(f Field) error {
		*fb = append(*fb, f)
		return nil
	})
}

// GetField returns a field by name. Returns an error if the field doesn't exists.
func (fb FieldBuffer) GetField(name string) (Field, error) {
	for _, f := range fb {
		if f.Name == name {
			return f, nil
		}
	}

	return Field{}, fmt.Errorf("field %q not found", name)
}

// Set replaces a field if it already exists or creates one if not.
func (fb *FieldBuffer) Set(f Field) {
	s := *fb
	for i := range s {
		if s[i].Name == f.Name {
			(*fb)[i] = f
			return
		}
	}

	fb.Add(f)
}

// Iterate goes through all the fields of the record and calls the given function by passing each one of them.
// If the given function returns an error, the iteration stops.
func (fb FieldBuffer) Iterate(fn func(Field) error) error {
	for _, f := range fb {
		err := fn(f)
		if err != nil {
			return err
		}
	}

	return nil
}

// Delete a field from the buffer.
func (fb *FieldBuffer) Delete(name string) error {
	s := *fb
	for i := range s {
		if s[i].Name == name {
			*fb = append(s[0:i], s[i+1:]...)
			return nil
		}
	}

	return fmt.Errorf("field %q not found", name)
}

// Replace the field with the given name by f.
func (fb *FieldBuffer) Replace(name string, f Field) error {
	s := *fb
	for i := range s {
		if s[i].Name == name {
			s[i] = f
			*fb = s
			return nil
		}
	}

	return fmt.Errorf("field %q not found", f.Name)
}

// NewFromMap creates a record from a map.
// Due to the way maps are designed, iteration order is not guaranteed.
func NewFromMap(m map[string]interface{}) Record {
	return mapRecord(m)
}

type mapRecord map[string]interface{}

var _ Record = (*mapRecord)(nil)

func (m mapRecord) Iterate(fn func(Field) error) error {
	for k, v := range m {
		f, err := NewField(k, v)
		if err != nil {
			return err
		}

		err = fn(f)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m mapRecord) GetField(name string) (Field, error) {
	v, ok := m[name]
	if !ok {
		return Field{}, fmt.Errorf("field %q not found", name)
	}
	return NewField(name, v)
}
