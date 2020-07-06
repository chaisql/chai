// Package document defines types to manipulate and compare documents and values.
package document

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ErrFieldNotFound must be returned by Document implementations, when calling the GetByField method and
// the field wasn't found in the document.
var (
	ErrFieldNotFound   = errors.New("field not found")
	ErrShortNotation   = errors.New("Short Notation")
	ErrCreateField     = errors.New("field must be created")
	ErrFieldReplaced   = errors.New("buffer is replaced")
	ErrValueNotSet     = errors.New("value not set")
	ErrIndexOutOfBound = errors.New("index out of bounds")
	ErrNotDocument     = errors.New("type must be  a document")
)

// A Document represents a group of key value pairs.
type Document interface {
	// Iterate goes through all the fields of the document and calls the given function by passing each one of them.
	// If the given function returns an error, the iteration stops.
	Iterate(fn func(field string, value Value) error) error
	// GetByField returns a value by field name.
	// Must return ErrFieldNotFound if the field doesnt exist.
	GetByField(field string) (Value, error)
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

//fieldValue  Document structure field and value pairs.
type fieldValue struct {
	Field string
	Value Value
}

// NewFieldBuffer creates a FieldBuffer.
func NewFieldBuffer() *FieldBuffer {
	return new(FieldBuffer)
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

var (
	errShortNotation = errors.New("Short Notation")
)

// ArrayFindIndex
func (path ValuePath) findIndexInPath() (int, error) {
	var err error
	var index int

	index = -1
	for _, p := range path {
		index, err = strconv.Atoi(p)
		if err == nil {
			return index, nil
		}

	}
	return index, err
}

// lastIndexOfPath return the last index of path.
func (path ValuePath) lastIndexOfPath() int {
	size := len(path)
	if size == 0 {
		return 0
	}

	return (size - 1)
}

// FieldValidator set field
func (path ValuePath) FieldValidator(v Value) (Value, int, error) {
	fmt.Printf("FieldValidator: v == %v && path == %s\n", v, path)
	d, _ := v.V.(Document)

	for i, p := range path {
		vv, err := d.GetByField(p)
		if err == nil {
			path = path[1:]
			fmt.Printf("FieldValidator: RETURN %v with path %s\n", v, path)
			return vv, i, err
		}
	}
	fmt.Printf("FieldValidator: Return err := %s\n", ErrFieldNotFound)

	return v, -1, ErrFieldNotFound

}

// IndexValidator check if the index is not out of range
func (path ValuePath) IndexValidator(a Array) (Value, int, error) {
	fmt.Printf("IndexValidator: path %s\n", path)
	index, err := path.findIndexInPath()
	if err != nil {
		fmt.Printf("IndexValidator: error %s\n", err)
		return NewZeroValue(ArrayValue), -1, err
	}

	v, err := a.GetByIndex(index)
	fmt.Printf("IndexValidator: = index %d %v and err %s\n", index, v, err)
	if err != nil {
		fmt.Printf("index validator Err := %s\n", ErrIndexOutOfBound)
		return NewZeroValue(ArrayValue), index, ErrIndexOutOfBound
	}

	fmt.Printf("IndexValidator: RETURN %v with path %s but must be %s\n", v, path, path[1:])
	return v, index, nil
}

//
func setDocumentValue(value Value, f string, reqValue Value) (Value, error) {
	d, err := value.ConvertToDocument()
	if err != nil {
		return value, err
	}
	fmt.Printf("setDocumentValue:change Value == %v at field %s, by value = %v\n", value, f, reqValue)
	var fbuf FieldBuffer
	err = d.Iterate(func(field string, va Value) error {
		if f == field {
			fbuf.Add(field, reqValue)
			return nil
		}

		fbuf.Add(field, va)
		return nil
	})
	return NewDocumentValue(fbuf), err
}

// SizeOfDoc return the size of Document.
func SizeOfDoc(d Document) int {
	var i int = 0
	d.Iterate(func(field string, v Value) error {
		i++
		return nil
	})
	return i
}


func NewFieldBufferByCopy(v Value) (FieldBuffer, error) {
	var buf FieldBuffer
	err := buf.Copy(v.V.(Document))
	if err != nil {
		return *NewFieldBuffer(), ErrNotDocument
	}

	return buf, nil
}

// IsUniqueField is the ValuePath contains an unique field to set <field>
func (path ValuePath) IsUniqueField() bool {
	if len(path) == 1 {
		return true
	}

	return false
}

// isLastField check if we reach the  field of ValuePath.
func (path ValuePath) isLastField() bool {
	if len(path) == 1 {
		return true
	}

	return false
}

// SetUniqueFieldOfDocument Add/Replace a value if the request is <Document.Field> or <Array.Index> or field
func (fb *FieldBuffer) SetUniqueFieldOfDocument(field string, reqValue Value) error {
	fmt.Printf("SetUniqueFieldOfDocument: : field  = %s by value %v\n", field, reqValue)
	_, err := fb.GetByField(field)
	switch err {
	case ErrFieldNotFound:
		fmt.Printf("SetUniqueFieldOfDocument: ErrFieldNotFound: fb.Add(field = %s, reqValue == %v\n", field, reqValue)
		fb.Add(field, reqValue)
		fmt.Printf("SetUniqueFieldOfDocument: : Final value  = %v\n", NewDocumentValue(fb))
		return nil
	case nil:
		fmt.Printf("SetUniqueFieldOfDocument: REPLACE %s\n", err)
		fb.Replace(field, reqValue)
		fmt.Printf("SetUniqueFieldOfDocument: : Final value  = %v\n", NewDocumentValue(fb))
		return nil
	default:
		fmt.Printf("SetUniqueFieldOfDocument: DEFAULT: fb.Add(field = %s, reqValue == %v\n", field, reqValue)
		return err
	}
}

// SetDocument reur
func (fb *FieldBuffer) SetDocument(v Value, path ValuePath, reqValue Value) (Value, error) {
	fmt.Printf("SetDocument: SetValue %v with path = %s into Value == %v\n", reqValue, path, v)
	fmt.Printf("SetDocument: V.TYPE == %s \n", v.Type)
	switch v.Type {
	case DocumentValue:
		fmt.Printf("SetDocument: DocumentValue: V == %v and V.Type == %s \n", v, v.Type)
		buf, err := NewFieldBufferByCopy(v)
		if err != nil {
			fmt.Printf("SetDocument: DocumentValue ERROR ==== > %s\n", err)

			return v, err
		}
		fmt.Printf("SetDocument: SetUniqueFieldOfDocument len path ==== > %d with path %s\n", len(path), path)
		if path.IsUniqueField() {
			fmt.Printf("SetDocument: SetUniqueFieldOfDocument ERROR ==== > %s\n", err)
			buf.SetUniqueFieldOfDocument(path[0], reqValue)
			return NewDocumentValue(buf), nil
		}

		va, err := buf.GetByField(path[0])
		fmt.Printf("SetDocument: DocumentValue V == %v and V.Type == %s and path %s\n", va, va.Type, path)
		fmt.Printf("############# VA  == %v #########################\n", va)

		va, err = buf.SetDocument(va, path[1:], reqValue)
		buf.SetUniqueFieldOfDocument(path[0], va)
		fmt.Printf("############# BUF  == %v #########################\n", NewDocumentValue(buf))

		return NewDocumentValue(buf), nil

	case ArrayValue:
		fmt.Printf("SetDocument: ArrayValue V == %v and V.Type == %s && path %s\n", v, v.Type, path)
		buf, err := NewValueBufferByCopy(v)
		if err != nil {
			return v, err
		}

		va, index, err := buf.GetValueFromString(path[0])
		if err != nil {
			return v, err
		}
		fmt.Printf("SetDocument: ArrayValue V == %v and V.Type == %s and index %d\n", va, va.Type, index)
		fmt.Printf("############# VA  == %v #########################\n", va)
		if len(path) == 1 {
			buf.Replace(index, reqValue)
			fmt.Printf("############# ARRAYVALUE:   BUF  == %v #########################\n", NewArrayValue(buf))
			return NewArrayValue(buf), nil
		}
		va, err = fb.SetDocument(va, path[1:], reqValue)
		buf.Replace(index, va)
		fmt.Printf("############# ARRAYVALUE: NOT LAST  BUF  == %v #########################\n", NewArrayValue(buf))
		return NewArrayValue(buf), nil
	}

	return v, nil
}

// Set replaces a field if it already exists or creates one if not.
func (fb *FieldBuffer) Set(path ValuePath, reqValue Value) error {
	if path.IsUniqueField() {
		return fb.SetUniqueFieldOfDocument(path[0], reqValue)
	}

	for i, field := range fb.fields {
		if path[0] == field.Field {
			v, err := fb.SetDocument(field.Value, path[1:], reqValue)
			if err != nil {
				return err
			}

			fb.fields[i].Value = v
			return nil
		}
	}

	return nil
}

// Iterate goes through all the fields of the document and calls the given function by passing each one of them.
// If the given function returns an error, the iteration stops.
func (fb FieldBuffer) Iterate(fn func(field string, value Value) error) error {
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

// GetFirstStringFromValuePath return the first string element of the valuePath
func (p ValuePath) GetFirstStringFromValuePath() string {
	return p[0]
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

//indemnisation.collectives@april.fr

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
