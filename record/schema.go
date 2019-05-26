package record

import (
	"errors"
	"fmt"
	"strings"

	"github.com/asdine/genji/field"
)

// Schema contains information about a table and its fields.
type Schema struct {
	Fields FieldBuffer
}

// Field returns a field information by name.
func (s *Schema) Field(fieldName string) (field.Field, error) {
	for i := range s.Fields {
		if s.Fields[i].Name == fieldName {
			return s.Fields[i], nil
		}
	}

	return field.Field{}, fmt.Errorf("field %s not found", fieldName)
}

// Validate a record against the schema. The record fields must be organized in the same order
// as the schema fields.
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

// Equal compares the other schema with s and returns true if they are equal.
func (s *Schema) Equal(other *Schema) bool {
	if len(s.Fields) != len(other.Fields) {
		return false
	}

	for i := range s.Fields {
		if s.Fields[i].Name != other.Fields[i].Name {
			return false
		}

		if s.Fields[i].Type != other.Fields[i].Type {
			return false
		}
	}

	return true
}

// String formats the schema into a coma separated list of fields.
// Each field is formatted as a combination of name and type separated by a colon.
func (s *Schema) String() string {
	var b strings.Builder
	for i, f := range s.Fields {
		b.WriteString(f.Name + ":" + f.Type.String())
		if i+1 < len(s.Fields) {
			b.WriteString(", ")
		}
	}

	return b.String()
}
