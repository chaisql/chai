// Package document defines types to manipulate and compare documents and values.
//
// Comparing values
//
// When comparing values, only compatible types can be compared together, otherwise the result
// of the comparison will always be false.
// Here is a list of types than can be compared with each other:
//
//   any integer			any integer
//   any integer			float64
//   float64			float64
//   string			string
//   string			bytes
//   bytes			bytes
//   bool			bool
//   null			null
package document

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
)

// ErrFieldNotFound must be returned by Document implementations, when calling the GetByField method and
// the field wasn't found in the document.
var ErrFieldNotFound = errors.New("field not found")

// A Document represents a group of key value pairs.
type Document interface {
	// Iterate goes through all the fields of the document and calls the given function by passing each one of them.
	// If the given function returns an error, the iteration stops.
	Iterate(fn func(field string, value Value) error) error
	// GetByField returns a value by field name.
	// Must return ErrFieldNotFound if the field doesnt exist.
	GetByField(field string) (Value, error)
}

// NewFromMap creates a document from a map.
// Due to the way maps are designed, iteration order is not guaranteed.
func NewFromMap(m map[string]interface{}) Document {
	return mapDocument(m)
}

type mapDocument map[string]interface{}

var _ Document = (*mapDocument)(nil)

func (m mapDocument) Iterate(fn func(f string, v Value) error) error {
	for mk, mv := range m {
		v, err := NewValue(mv)
		if err != nil {
			return err
		}

		err = fn(mk, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m mapDocument) GetByField(field string) (Value, error) {
	v, ok := m[field]
	if !ok {
		return Value{}, ErrFieldNotFound
	}
	return NewValue(v)
}

// NewFromStruct creates a document from a struct using reflection.
func NewFromStruct(s interface{}) (Document, error) {
	ref := reflect.Indirect(reflect.ValueOf(s))

	if !ref.IsValid() || ref.Kind() != reflect.Struct {
		return nil, errors.New("expected struct or pointer to struct")
	}

	return structDocument{ref: ref}, nil
}

// this error is used to skip struct or array fields that are not supported.
var errUnsupportedType = errors.New("unsupported type")

type structDocument struct {
	ref reflect.Value
}

var _ Document = (*structDocument)(nil)

func (s structDocument) Iterate(fn func(f string, v Value) error) error {
	l := s.ref.NumField()

	tp := s.ref.Type()

	for i := 0; i < l; i++ {
		sf := tp.Field(i)
		if sf.Anonymous || sf.PkgPath != "" {
			continue
		}

		var name string
		if gtag, ok := sf.Tag.Lookup("genji"); ok {
			if gtag == "-" {
				continue
			}

			name = gtag
		} else {
			name = strings.ToLower(sf.Name)
		}

		f := s.ref.Field(i)

		v, err := reflectValueToValue(f)
		if err == errUnsupportedType {
			continue
		}
		if err != nil {
			return err
		}

		err = fn(name, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s structDocument) GetByField(field string) (Value, error) {
	tp := s.ref.Type()

	var sf reflect.StructField
	var ok bool

	ln := tp.NumField()
	for i := 0; i < ln; i++ {
		sf = tp.Field(i)
		if gtag, found := sf.Tag.Lookup("genji"); found && gtag == field {
			ok = true
			break
		}
		if strings.ToLower(sf.Name) == field {
			ok = true
			break
		}
	}

	if !ok || sf.Anonymous || sf.PkgPath != "" {
		return Value{}, ErrFieldNotFound
	}

	v := s.ref.FieldByName(sf.Name)
	if !v.IsValid() {
		return Value{}, ErrFieldNotFound
	}

	return reflectValueToValue(v)
}

func reflectValueToValue(v reflect.Value) (Value, error) {
	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			return NewNullValue(), nil
		}
		return reflectValueToValue(reflect.Indirect(v))
	case reflect.Struct:
		return NewDocumentValue(&structDocument{ref: v}), nil
	case reflect.String:
		return NewTextValue(v.String()), nil
	case reflect.Slice:
		if reflect.TypeOf(v.Interface()).Elem().Kind() == reflect.Uint8 {
			return NewBytesValue(v.Bytes()), nil
		}
		if v.IsNil() {
			return NewNullValue(), nil
		}
		return NewArrayValue(&sliceArray{ref: v}), nil
	case reflect.Array:
		return NewArrayValue(&sliceArray{ref: v}), nil
	case reflect.Bool:
		return NewBoolValue(v.Bool()), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intToValue(v.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		x := v.Uint()
		if x > math.MaxInt64 {
			return Value{}, fmt.Errorf("cannot convert unsigned integer struct field to int64: %d out of range", x)
		}

		return intToValue(int64(x)), nil
	case reflect.Float32, reflect.Float64:
		return NewFloat64Value(v.Float()), nil
	case reflect.Interface:
		if v.IsNil() {
			return NewNullValue(), nil
		}
		return reflectValueToValue(v.Elem())
	}
	return Value{}, errUnsupportedType
}

// A Keyer returns the key identifying documents in their storage.
// This is usually implemented by documents read from storages.
type Keyer interface {
	Key() []byte
}

// FieldBuffer stores a group of fields in memory. It implements the Document interface.
type FieldBuffer struct {
	fields []fieldValue
}

// NewFieldBuffer creates a FieldBuffer.
func NewFieldBuffer() *FieldBuffer {
	return new(FieldBuffer)
}

type fieldValue struct {
	Field string
	Value Value
}

// Add a field to the buffer.
func (fb *FieldBuffer) Add(field string, v Value) *FieldBuffer {
	fb.fields = append(fb.fields, fieldValue{field, v})
	return fb
}

// ScanDocument copies all the fields of d to the buffer.
func (fb *FieldBuffer) ScanDocument(d Document) error {
	return d.Iterate(func(f string, v Value) error {
		fb.Add(f, v)
		return nil
	})
}

// GetByField returns a value by field. Returns an error if the field doesn't exists.
func (fb FieldBuffer) GetByField(field string) (Value, error) {
	for _, fv := range fb.fields {
		if fv.Field == field {
			return fv.Value, nil
		}
	}

	return Value{}, ErrFieldNotFound
}

// Set replaces a field if it already exists or creates one if not.
func (fb *FieldBuffer) Set(f string, v Value) {
	for i := range fb.fields {
		if fb.fields[i].Field == f {
			fb.fields[i].Value = v
			return
		}
	}

	fb.Add(f, v)
}

// Iterate goes through all the fields of the document and calls the given function by passing each one of them.
// If the given function returns an error, the iteration stops.
func (fb FieldBuffer) Iterate(fn func(f string, v Value) error) error {
	for _, fv := range fb.fields {
		err := fn(fv.Field, fv.Value)
		if err != nil {
			return err
		}
	}

	return nil
}

// Delete a field from the buffer.
func (fb *FieldBuffer) Delete(field string) error {
	for i := range fb.fields {
		if fb.fields[i].Field == field {
			fb.fields = append(fb.fields[0:i], fb.fields[i+1:]...)
			return nil
		}
	}

	return ErrFieldNotFound
}

// Replace the value of the field by v.
func (fb *FieldBuffer) Replace(field string, v Value) error {
	for i := range fb.fields {
		if fb.fields[i].Field == field {
			fb.fields[i].Value = v
			return nil
		}
	}

	return ErrFieldNotFound
}

// Copy deep copies every value of the document to the buffer.
// If a value is a document or an array, it will be stored as a FieldBuffer or ValueBuffer respectively.
func (fb *FieldBuffer) Copy(d Document) error {
	err := fb.ScanDocument(d)
	if err != nil {
		return err
	}

	for i, f := range fb.fields {
		switch f.Value.Type {
		case DocumentValue:
			var buf FieldBuffer
			err = buf.Copy(f.Value.V.(Document))
			if err != nil {
				return err
			}

			fb.fields[i].Value = NewDocumentValue(&buf)
		case ArrayValue:
			var buf ValueBuffer
			err = buf.Copy(f.Value.V.(Array))
			if err != nil {
				return err
			}

			fb.fields[i].Value = NewArrayValue(&buf)
		}
	}

	return nil
}

// Len of the buffer.
func (fb FieldBuffer) Len() int {
	return len(fb.fields)
}

// MarshalJSON implements the json.Marshaler interface.
func (fb *FieldBuffer) MarshalJSON() ([]byte, error) {
	return json.Marshal(jsonDocument{Document: fb})
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (fb *FieldBuffer) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))

	t, err := dec.Token()
	if err == io.EOF {
		return err
	}

	return parseJSONDocument(dec, t, fb)
}

// Reset the buffer.
func (fb *FieldBuffer) Reset() {
	fb.fields = fb.fields[:0]
}

// A ValuePath represents the path to a particular value within a document.
type ValuePath []string

// NewValuePath takes a string representation of a value path and returns a ValuePath.
// It assumes the separator is a dot.
func NewValuePath(p string) ValuePath {
	return strings.Split(p, ".")
}

// String joins all the chunks of the path using the dot separator.
// It implements the Stringer interface.
func (p ValuePath) String() string {
	return strings.Join(p, ".")
}

// GetValue from a document.
func (p ValuePath) GetValue(d Document) (Value, error) {
	return p.getValueFromDocument(d)
}

func (p ValuePath) getValueFromDocument(d Document) (Value, error) {
	if len(p) == 0 {
		return Value{}, errors.New("empty valuepath")
	}

	v, err := d.GetByField(p[0])
	if err != nil {
		return Value{}, err
	}

	return p.getValueFromValue(v)
}

func (p ValuePath) getValueFromArray(a Array) (Value, error) {
	if len(p) == 0 {
		return Value{}, errors.New("empty valuepath")
	}

	i, err := strconv.Atoi(p[0])
	if err != nil {
		return Value{}, err
	}

	v, err := a.GetByIndex(i)
	if err != nil {
		return Value{}, err
	}

	return p.getValueFromValue(v)
}

func (p ValuePath) getValueFromValue(v Value) (Value, error) {
	if len(p) == 1 {
		return v, nil
	}

	switch v.Type {
	case DocumentValue:
		if len(p) == 1 {
			return v, nil
		}

		d, err := v.ConvertToDocument()
		if err != nil {
			return Value{}, err
		}

		return p[1:].getValueFromDocument(d)
	case ArrayValue:

		a, err := v.ConvertToArray()
		if err != nil {
			return Value{}, err
		}

		return p[1:].getValueFromArray(a)
	}

	return Value{}, ErrFieldNotFound
}
