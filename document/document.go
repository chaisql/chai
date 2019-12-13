// Package document defines types to manipulate, encode and compare documents and values.
//
// Encoding values
//
// Each type is encoded in a way that allows ordering to be preserved. That way, vA < vB,
// where vA and vB are two unencoded values of the same type, then eA < eB, where eA and eB
// are the respective encoded values of vA and vB.
//
// Comparing values
//
// When comparing values, only compatible types can be compared together, otherwise the result
// of the comparison will always be false.
// Here is a list of types than can be compared with each other:
//
//   any integer	any integer
//   any integer	float64
//   float64		float64
//   string			string
//   string			bytes
//   bytes			bytes
//   bool			bool
//	 null			null
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

		f := s.ref.Field(i)

		v, err := reflectValueToValue(f)
		if err == errUnsupportedType {
			continue
		}
		if err != nil {
			return err
		}

		err = fn(strings.ToLower(sf.Name), v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s structDocument) GetByField(field string) (Value, error) {
	tp := s.ref.Type()
	sf, ok := tp.FieldByName(field)
	if !ok || sf.Anonymous || sf.PkgPath != "" {
		return Value{}, ErrFieldNotFound
	}

	v := s.ref.FieldByName(field)
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
		return NewStringValue(v.String()), nil
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
	case reflect.Int8:
		return NewInt8Value(int8(v.Int())), nil
	case reflect.Int16:
		return NewInt16Value(int16(v.Int())), nil
	case reflect.Int32:
		return NewInt32Value(int32(v.Int())), nil
	case reflect.Int64:
		return NewInt64Value(v.Int()), nil
	case reflect.Int:
		return NewIntValue(int(v.Int())), nil
	case reflect.Uint8:
		return NewUint8Value(uint8(v.Uint())), nil
	case reflect.Uint16:
		return NewUint16Value(uint16(v.Uint())), nil
	case reflect.Uint32:
		return NewUint32Value(uint32(v.Uint())), nil
	case reflect.Uint64:
		return NewUint64Value(v.Uint()), nil
	case reflect.Uint:
		return NewUintValue(uint(v.Uint())), nil
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

// ScanDocument copies all the fields of r to the buffer.
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

	// expecting a '{'
	if d, ok := t.(json.Delim); !ok || d.String() != "{" {
		return fmt.Errorf("found %q, expected '{'", d.String())
	}

	for dec.More() {
		err = fb.parseJSONKV(dec)
		if err != nil {
			return err
		}
	}

	t, err = dec.Token()
	if err == io.EOF {
		return fmt.Errorf("found %q, expected '}'", err)
	}

	// expecting a '}'
	if d, ok := t.(json.Delim); !ok || d.String() != "}" {
		return fmt.Errorf("found %q, expected '}'", d.String())
	}

	return nil
}

func (fb *FieldBuffer) parseJSONKV(dec *json.Decoder) error {
	// parse the key, it must be a string
	t, err := dec.Token()
	if err != nil {
		return err
	}

	k, ok := t.(string)
	if !ok {
		return fmt.Errorf("found %q, expected '{'", t)
	}

	v, err := parseJSONValue(dec)
	if err != nil {
		return err
	}

	fb.Add(k, v)
	return nil
}

func parseJSONValue(dec *json.Decoder) (Value, error) {
	// ensure the decoder parses numbers as the json.Number type
	dec.UseNumber()

	// parse the first token to determine which type is it
	t, err := dec.Token()
	if err != nil {
		return Value{}, err
	}

	switch tt := t.(type) {
	case string:
		return NewStringValue(tt), nil
	case bool:
		return NewBoolValue(tt), nil
	case nil:
		return NewNullValue(), nil
	case json.Number:
		i, err := tt.Int64()
		if err != nil {
			// if it's too big to fit in an int64, perhaps it can fit in a uint64
			ui, err := strconv.ParseUint(tt.String(), 10, 64)
			if err == nil {
				return NewUint64Value(ui), nil
			}

			// let's try parsing this as a floating point number
			f, err := tt.Float64()
			if err != nil {
				return Value{}, err
			}

			return NewFloat64Value(f), nil
		}

		switch {
		case i >= math.MinInt8 && i <= math.MaxInt8:
			return NewInt8Value(int8(i)), nil
		case i >= math.MinInt16 && i <= math.MaxInt16:
			return NewInt16Value(int16(i)), nil
		case i >= math.MinInt32 && i <= math.MaxInt32:
			return NewInt32Value(int32(i)), nil
		default:
			return NewInt64Value(int64(i)), nil
		}
	case json.Delim:
		switch tt {
		case ']', '}':
			return Value{}, fmt.Errorf("found %q, expected '{' or '['", tt)
		case '[':
			buf := NewValueBuffer()
			for dec.More() {
				v, err := parseJSONValue(dec)
				if err != nil {
					return Value{}, err
				}
				buf = buf.Append(v)
			}

			// expecting ']'
			t, err = dec.Token()
			if err != nil {
				return Value{}, err
			}
			if d, ok := t.(json.Delim); !ok || d != ']' {
				return Value{}, fmt.Errorf("found %q, expected ']'", tt)
			}

			return NewArrayValue(buf), nil
		case '{':
			buf := NewFieldBuffer()
			for dec.More() {
				err := buf.parseJSONKV(dec)
				if err != nil {
					return Value{}, err
				}
			}

			// expecting '}'
			t, err = dec.Token()
			if err != nil {
				return Value{}, err
			}
			if d, ok := t.(json.Delim); !ok || d != '}' {
				return Value{}, fmt.Errorf("found %q, expected '}'", tt)
			}

			return NewDocumentValue(buf), nil
		}
	}

	return Value{}, nil
}

// Less reports whether the element with
// index i should sort before the element with index j.
// It implements the sort.Interface interface.
func (fb FieldBuffer) Less(i, j int) bool {
	return strings.Compare(fb.fields[i].Field, fb.fields[j].Field) < 0
}

// Swap swaps the elements with indexes i and j.
// It implements the sort.Interface interface.
func (fb *FieldBuffer) Swap(i, j int) {
	fb.fields[i], fb.fields[j] = fb.fields[j], fb.fields[i]
}

// Reset the buffer.
func (fb *FieldBuffer) Reset() {
	fb.fields = fb.fields[:0]
}

// ToJSON encodes d to w in JSON.
func ToJSON(w io.Writer, d Document) error {
	return json.NewEncoder(w).Encode(jsonDocument{d})
}

// ArrayToJSON encodes a to w in JSON.
func ArrayToJSON(w io.Writer, a Array) error {
	return json.NewEncoder(w).Encode(jsonArray{a})
}

type jsonDocument struct {
	Document
}

func (j jsonDocument) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteByte('{')

	var notFirst bool
	err := j.Document.Iterate(func(f string, v Value) error {
		if notFirst {
			buf.WriteByte(',')
		}
		notFirst = true

		buf.WriteByte('"')
		buf.WriteString(f)
		buf.WriteString(`":`)

		data, err := v.MarshalJSON()
		if err != nil {
			return err
		}
		_, err = buf.Write(data)
		return err
	})
	if err != nil {
		return nil, err
	}

	buf.WriteByte('}')

	return buf.Bytes(), nil
}

type jsonArray struct {
	Array
}

func (j jsonArray) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteByte('[')
	var notFirst bool
	err := j.Array.Iterate(func(i int, v Value) error {
		if notFirst {
			buf.WriteByte(',')
		}
		notFirst = true

		data, err := v.MarshalJSON()
		if err != nil {
			return err
		}

		_, err = buf.Write(data)
		return err
	})
	if err != nil {
		return nil, err
	}
	buf.WriteByte(']')
	return buf.Bytes(), nil
}

// An Array contains a set of values.
type Array interface {
	// Iterate goes through all the values of the array and calls the given function by passing each one of them.
	// If the given function returns an error, the iteration stops.
	Iterate(fn func(i int, value Value) error) error
	// GetByIndex returns a value by index of the array.
	GetByIndex(i int) (Value, error)
}
