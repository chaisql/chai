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
	"reflect"
	"strings"
)

// A Document represents a group of fields.
type Document interface {
	// Iterate goes through all the fields of the document and calls the given function by passing each one of them.
	// If the given function returns an error, the iteration stops.
	Iterate(fn func(field string, value Value) error) error
	// GetByField returns a value by field name.
	GetByField(field string) (Value, error)
}

// A Keyer returns the key identifying documents in their storage.
// This is usually implemented by documents read from storages.
type Keyer interface {
	Key() []byte
}

// A Scanner can iterate over a document and scan all the fields.
type Scanner interface {
	ScanDocument(Document) error
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
func (fb *FieldBuffer) ScanDocument(r Document) error {
	return r.Iterate(func(f string, v Value) error {
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

	return Value{}, fmt.Errorf("field %q not found", field)
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

	return fmt.Errorf("field %q not found", field)
}

// Replace the value of the field by v.
func (fb *FieldBuffer) Replace(field string, v Value) error {
	for i := range fb.fields {
		if fb.fields[i].Field == field {
			fb.fields[i].Value = v
			return nil
		}
	}

	return fmt.Errorf("field %q not found", field)
}

func (fb FieldBuffer) Len() int {
	return len(fb.fields)
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
		return Value{}, fmt.Errorf("field %q not found", field)
	}
	return NewValue(v)
}

// Dump is a helper that dumps the name, type and value of each field of a document into the given writer.
func Dump(w io.Writer, r Document) error {
	return r.Iterate(func(f string, v Value) error {
		x, err := v.Decode()
		fmt.Fprintf(w, "%s(%s): %#v\n", f, v.Type, x)
		return err
	})
}

// ToJSON encodes r to w in JSON.
func ToJSON(w io.Writer, r Document) error {
	return json.NewEncoder(w).Encode(jsonDocument{r})
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

		var x interface{}
		var err error

		if v.Type == DocumentValue {
			d, err := v.DecodeToDocument()
			if err != nil {
				return err
			}
			x = &jsonDocument{d}
		} else {
			x, err = v.Decode()
		}
		if err != nil {
			return err
		}
		mv, err := json.Marshal(x)
		if err != nil {
			return err
		}

		buf.Write(mv)
		return nil
	})
	if err != nil {
		return nil, err
	}

	buf.WriteByte('}')

	return buf.Bytes(), nil
}

// ToMap decodes the document into a map. m must be already allocated.
func ToMap(r Document, m map[string]interface{}) error {
	err := r.Iterate(func(f string, v Value) error {
		var err error
		m[f], err = v.Decode()
		return err
	})

	return err
}

// Scan a document into the given variables. Each variable must be a pointer to
// types supported by Genji.
// If only one target is provided, the target can also be a Scanner,
// a map[string]interface{} or a pointer to map[string]interface{}.
func Scan(r Document, targets ...interface{}) error {
	var i int

	if len(targets) == 1 {
		if rs, ok := targets[0].(Scanner); ok {
			return rs.ScanDocument(r)
		}
		if mPtr, ok := targets[0].(*map[string]interface{}); ok {
			if *mPtr == nil {
				*mPtr = make(map[string]interface{})
			}

			return ToMap(r, *mPtr)
		}
		if m, ok := targets[0].(map[string]interface{}); ok {
			return ToMap(r, m)
		}
	}

	return r.Iterate(func(f string, v Value) error {
		if i >= len(targets) {
			return errors.New("target list too small")
		}

		ref := reflect.ValueOf(targets[i])

		if !ref.IsValid() || ref.Kind() != reflect.Ptr {
			return errors.New("target must be pointer to a valid Go type")
		}

		switch t := targets[i].(type) {
		case *uint:
			x, err := v.DecodeToUint()
			if err != nil {
				return err
			}

			*t = x
		case *uint8:
			x, err := v.DecodeToUint8()
			if err != nil {
				return err
			}

			*t = x
		case *uint16:
			x, err := v.DecodeToUint16()
			if err != nil {
				return err
			}

			*t = x
		case *uint32:
			x, err := v.DecodeToUint32()
			if err != nil {
				return err
			}

			*t = x
		case *uint64:
			x, err := v.DecodeToUint64()
			if err != nil {
				return err
			}

			*t = x
		case *int:
			x, err := v.DecodeToInt()
			if err != nil {
				return err
			}

			*t = x
		case *int8:
			x, err := v.DecodeToInt8()
			if err != nil {
				return err
			}

			*t = x
		case *int16:
			x, err := v.DecodeToInt16()
			if err != nil {
				return err
			}

			*t = x
		case *int32:
			x, err := v.DecodeToInt32()
			if err != nil {
				return err
			}

			*t = x
		case *int64:
			x, err := v.DecodeToInt64()
			if err != nil {
				return err
			}

			*t = x
		case *float32:
			x, err := v.DecodeToFloat64()
			if err != nil {
				return err
			}

			*t = float32(x)
		case *float64:
			x, err := v.DecodeToFloat64()
			if err != nil {
				return err
			}

			*t = x
		case *string:
			x, err := v.DecodeToString()
			if err != nil {
				return err
			}

			*t = x
		case *[]byte:
			x, err := v.DecodeToBytes()
			if err != nil {
				return err
			}

			*t = x
		case *bool:
			x, err := v.DecodeToBool()
			if err != nil {
				return err
			}

			*t = x
		default:
			return fmt.Errorf("unsupported type %T", t)
		}
		i++
		return nil
	})
}
