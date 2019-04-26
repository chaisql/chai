package record

import (
	"errors"
	"fmt"

	"github.com/asdine/genji/field"
)

// Schema contains information about a table and its fields.
type Schema struct {
	Fields []field.Field
}

func (s *Schema) Field(fieldName string) (field.Field, error) {
	for i := range s.Fields {
		if s.Fields[i].Name == fieldName {
			return s.Fields[i], nil
		}
	}

	return field.Field{}, fmt.Errorf("field %s not found", fieldName)
}

func (s *Schema) Validate(rec Record) error {
	var i int

	err := rec.Iterate(func(f field.Field) error {
		if i >= len(s.Fields) {
			return errors.New("record contains too many fields")
		}

		sf := s.Fields[i]
		if sf.Name != f.Name || sf.Type != f.Type {
			return fmt.Errorf("field should be '%s' of type '%d', got '%s' of type '%d'", sf.Name, sf.Type, f.Name, f.Type)
		}

		i++
		return nil
	})
	if err != nil {
		return err
	}

	if i < len(s.Fields) {
		return errors.New("record contains too few fields")
	}

	return nil
}

type SchemaRecord struct {
	*Schema
	TableName string
}

// Pk returns the TableName as the primary key.
func (s *SchemaRecord) Pk() ([]byte, error) {
	return []byte(s.TableName), nil
}

// Field implements the field method of the Record interface.
func (s *SchemaRecord) Field(name string) (field.Field, error) {
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
func (s *SchemaRecord) Iterate(fn func(field.Field) error) error {
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
func (s *SchemaRecord) ScanRecord(rec Record) error {
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

	if s.Schema == nil {
		s.Schema = new(Schema)
	}

	ec := EncodedRecord(f.Data)
	return ec.Iterate(func(f field.Field) error {
		s.Fields = append(s.Fields, f)
		return nil
	})
}
