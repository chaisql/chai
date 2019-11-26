// Package record defines interfaces, implementations and helpers to manipulate and encode records.
package record

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/asdine/genji/value"
	"io"
	"reflect"
	"strings"
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

func (fb FieldBuffer) Len() int {
	return len(fb)
}

// Less reports whether the element with
// index i should sort before the element with index j.
// It implements the sort.Interface interface.
func (fb FieldBuffer) Less(i, j int) bool {
	return strings.Compare(fb[i].Name, fb[j].Name) < 0
}

// Swap swaps the elements with indexes i and j.
// It implements the sort.Interface interface.
func (fb *FieldBuffer) Swap(i, j int) {
	(*fb)[i], (*fb)[j] = (*fb)[j], (*fb)[i]
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

// Dump is a helper that dumps the name, type and value of each field of a record into the given writer.
func Dump(w io.Writer, r Record) error {
	return r.Iterate(func(f Field) error {
		v, err := f.Decode()
		fmt.Fprintf(w, "%s(%s): %#v\n", f.Name, f.Type, v)
		return err
	})
}

// ToJSON encodes r to w in JSON.
func ToJSON(w io.Writer, r Record) error {
	return json.NewEncoder(w).Encode(jsonRecord{r})
}

type jsonRecord struct {
	Record
}

func (j jsonRecord) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteByte('{')

	var notFirst bool
	err := j.Record.Iterate(func(f Field) error {
		if notFirst {
			buf.WriteByte(',')
		}
		notFirst = true

		buf.WriteByte('"')
		buf.WriteString(f.Name)
		buf.WriteString(`":`)

		var v interface{}
		var err error

		if f.Type == value.Object {
			v = &jsonRecord{f.nestedRecord}
		} else {
			v, err = f.Decode()
		}
		if err != nil {
			return err
		}
		mv, err := json.Marshal(v)
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

// ToMap decodes the record into a map. m must be already allocated.
func ToMap(r Record, m map[string]interface{}) error {
	err := r.Iterate(func(f Field) error {
		var err error
		m[f.Name], err = f.Decode()
		return err
	})

	return err
}

// Scan a record into the given variables. Each variable must be a pointer to
// types supported by Genji.
// If only one target is provided, the target can also be a Scanner,
// a map[string]interface{} or a pointer to map[string]interface{}.
func Scan(r Record, targets ...interface{}) error {
	var i int

	if len(targets) == 1 {
		if rs, ok := targets[0].(Scanner); ok {
			return rs.ScanRecord(r)
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

	return r.Iterate(func(f Field) error {
		if i >= len(targets) {
			return errors.New("target list too small")
		}

		ref := reflect.ValueOf(targets[i])

		if !ref.IsValid() || ref.Kind() != reflect.Ptr {
			return errors.New("target must be pointer to a valid Go type")
		}

		switch t := targets[i].(type) {
		case *uint:
			x, err := f.DecodeToUint()
			if err != nil {
				return err
			}

			*t = x
		case *uint8:
			x, err := f.DecodeToUint8()
			if err != nil {
				return err
			}

			*t = x
		case *uint16:
			x, err := f.DecodeToUint16()
			if err != nil {
				return err
			}

			*t = x
		case *uint32:
			x, err := f.DecodeToUint32()
			if err != nil {
				return err
			}

			*t = x
		case *uint64:
			x, err := f.DecodeToUint64()
			if err != nil {
				return err
			}

			*t = x
		case *int:
			x, err := f.DecodeToInt()
			if err != nil {
				return err
			}

			*t = x
		case *int8:
			x, err := f.DecodeToInt8()
			if err != nil {
				return err
			}

			*t = x
		case *int16:
			x, err := f.DecodeToInt16()
			if err != nil {
				return err
			}

			*t = x
		case *int32:
			x, err := f.DecodeToInt32()
			if err != nil {
				return err
			}

			*t = x
		case *int64:
			x, err := f.DecodeToInt64()
			if err != nil {
				return err
			}

			*t = x
		case *float32:
			x, err := f.DecodeToFloat64()
			if err != nil {
				return err
			}

			*t = float32(x)
		case *float64:
			x, err := f.DecodeToFloat64()
			if err != nil {
				return err
			}

			*t = x
		case *string:
			x, err := f.DecodeToString()
			if err != nil {
				return err
			}

			*t = x
		case *[]byte:
			x, err := f.DecodeToBytes()
			if err != nil {
				return err
			}

			*t = x
		case *bool:
			x, err := f.DecodeToBool()
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
