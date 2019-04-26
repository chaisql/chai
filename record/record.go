package record

import (
	"errors"

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

// A Decoder decodes encoded records.
type Decoder interface {
	Decode([]byte) (Record, error)
}

// An Encoder encodes records.
type Encoder interface {
	Encode(Record) ([]byte, error)
}

// A Codec can encode and decode records.
type Codec interface {
	Encoder
	Decoder
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

	return field.Field{}, errors.New("not found")
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
