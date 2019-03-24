package record

import (
	"errors"

	"github.com/asdine/genji/field"
)

// Schema contains information about a table and its fields.
type Schema struct {
	TableName string
	Fields    []field.Field
}

// Field implements the field method of the Record interface.
func (s *Schema) Field(name string) (field.Field, error) {
	switch name {
	case "TableName":
		return field.Field{
			Name: "TableName",
			Type: field.String,
			Data: []byte(s.TableName),
		}, nil
	case "Fields":
		data, err := Encode(FieldBuffer(s.Fields))
		if err != nil {
			return field.Field{}, err
		}

		return field.Field{
			Name: "Fields",
			Type: field.String,
			Data: data,
		}, nil
	}

	return field.Field{}, errors.New("unknown field")
}

// Iterate through all the fields one by one and pass each of them to the given function.
// It the given function returns an error, the iteration is interrupted.
func (s *Schema) Iterate(fn func(field.Field) error) error {
	var err error
	var f field.Field

	f, err = s.Field("TableName")
	if err != nil {
		return err
	}

	err = fn(f)
	if err != nil {
		return err
	}

	f, err = s.Field("Fields")
	if err != nil {
		return err
	}

	err = fn(f)
	if err != nil {
		return err
	}

	return nil
}

// ScanRecord extracts fields from record and assigns them to the struct fields.
func (s *Schema) ScanRecord(rec Record) error {
	var f field.Field
	var err error

	f, err = rec.Field("TableName")
	if err != nil {
		return err
	}
	s.TableName = string(f.Data)

	f, err = rec.Field("Fields")
	if err != nil {
		return err
	}

	ec := rec.(EncodedRecord)
	return ec.Iterate(func(f field.Field) error {
		s.Fields = append(s.Fields, f)
		return nil
	})
}
