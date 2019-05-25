package record

import (
	"fmt"

	"github.com/asdine/genji/field"
)

// A Record holds a group of fields.
type Record interface {
	Field(string) (field.Field, error)
	Iterate(func(field.Field) error) error
}

// A Scanner can iterate over a record and scan all the fields.
type Scanner interface {
	ScanRecord(Record) error
}

// FieldBuffer contains a list of fields. It implements the Record interface.
type FieldBuffer []field.Field

func (fb *FieldBuffer) Add(f field.Field) {
	*fb = append(*fb, f)
}

func (fb *FieldBuffer) ScanRecord(r Record) error {
	return r.Iterate(func(f field.Field) error {
		*fb = append(*fb, f)
		return nil
	})
}

func (fb FieldBuffer) Field(name string) (field.Field, error) {
	for _, f := range fb {
		if f.Name == name {
			return f, nil
		}
	}

	return field.Field{}, fmt.Errorf("field %q not found", name)
}

func (fb FieldBuffer) Set(f field.Field) {
	for i := range fb {
		if fb[i].Name == f.Name {
			fb[i] = f
			return
		}
	}

	fb.Add(f)
}

func (fb FieldBuffer) Iterate(fn func(field.Field) error) error {
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
