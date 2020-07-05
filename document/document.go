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
	ErrValueNotSet     = errors.New("value not set")
	ErrIndexOutOfBound = errors.New("index out of bounds")
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
func FieldValidator(v Value, path ValuePath) (Value, int, error) {
	fmt.Printf("FieldValidator: v == %v && path == %s\n", v, path)
	d, _ := v.V.(Document)

	for i, p := range path {
		vv, err := d.GetByField(p)
		if err == nil {
			return vv, i, err
		}
	}
	fmt.Printf("FieldValidator: Return err := %s\n", ErrFieldNotFound)
	return v, -1, ErrFieldNotFound

}

// IndexValidator check if the index is not out of range
func IndexValidator(path ValuePath, a Array) (Value, int, error) {

	index, err := path.findIndexInPath()
	if err != nil {
		fmt.Printf("IndexValidator: error %s\n", err)
		return NewZeroValue(ArrayValue), -1, err
	}

	v, err := a.GetByIndex(index)
	fmt.Printf("IndexValidator: = %v and err %s\n", v, err)
	if err != nil {
		fmt.Printf("index validator Err := %s\n", err)
		return NewZeroValue(ArrayValue), index, ErrIndexOutOfBound
	}

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

// AddFieldToArray add a field in unique of document
func (fb *FieldBuffer) AddFieldToArray(value Value, field string, ReqField string, reqValue Value) error {
	var b ValueBuffer
	err := b.Copy(value.V.(Array))
	if err != nil {
		fb.Add(field, NewArrayValue(&b))
		fb.Delete(field)
		return err
	}

	fb.Add(field, NewArrayValue(&b))
	fb.Add(ReqField, reqValue)
	fb.Delete(field)

	return nil
}

func replaceValue(v Value, path ValuePath, reqValue Value) (Value, error) {
	switch v.Type {
	case DocumentValue:
		var buf FieldBuffer
		err := buf.Copy(v.V.(Document))
		if err != nil {
			return v, err
		}

		v, err = buf.SetDocument(v, path[1:], reqValue)
		if err != nil {
			return v, err
		}

		return v, nil
	case ArrayValue:
		var buf ValueBuffer
		err := buf.Copy(v.V.(Array))
		if err != nil {
			return v, err
		}
		i, err := strconv.Atoi(path[0])
		buf.Replace(i, reqValue)
		return NewArrayValue(buf), nil
	}

	return v, errors.New("type must be an array or a document")
}

// IsUniqueField is the ValuePath contains an unique field to set <field>
func (path ValuePath) IsUniqueField() bool {
	if len(path) == 1 {
		return true
	}

	return false
}

// isOneNestedField check if the Value path request just one field. Simple dot notatino <Document>.<field> or <Array>.>index>
func (path ValuePath) isOneNestedField() bool {
	if len(path) == 2 {
		return true
	}

	return false
}

// isLastField check if we reach the  field of ValuePath.
func (path ValuePath) isLastField() bool {
	indexOfLastField := path.lastIndexOfPath()
	if indexOfLastField == 1 {
		return true
	}

	return false
}

// SetDocument reur
func (fb *FieldBuffer) SetDocument(v Value, path ValuePath, reqValue Value) (Value, error) {
	fmt.Printf("ReplaceFieldValue: Replace value %v with path = %s by Value == %v\n", v, path, reqValue)
	last := path.lastIndexOfPath()
	fmt.Printf("ReplaceFieldValue: V == %v and V.Type == %s \n", v, v.Type)
	switch v.Type {
	case DocumentValue:
		var fbuf FieldBuffer
		err := fbuf.Copy(v.V.(Document))
		if err != nil {
			return v, nil
		}
		if path.isOneNestedField() {
			switch v.Type {
			case ArrayValue:
				buf, err := NewValueBufferByCopy(v)
				if err != nil {
					return v, err
				}
				_, index, err := IndexValidator(path, buf)
				if err != nil {
					return err
				}
				buf.Replace(index, reqValue)
				return NewDocumentValue(fbuf), nil
			}
			err := fbuf.SetUniqueFieldOfDocument(path[1], reqValue)
			return NewDocumentValue(fbuf), err
		}

		fmt.Printf("ReplaceFieldValue: DocumentValue: last == %d and path %s\n", last, path)
		nextField := 0

		if path.isLastField() {
			_, _, err := FieldValidator(v, path[1:])
			switch err {
			case ErrFieldNotFound:
				fmt.Printf("ReplaceFieldValue: fb %v\n", NewDocumentValue(fb))
				return NewDocumentValue(fbuf), ErrCreateField
			case nil:
				return NewDocumentValue(fbuf), nil
			}
		}

		if !path.isLastField() {
			vv, nextField, err := FieldValidator(v, path[nextField:])
			if err != nil {
				return v, err
			}
			vv, err = fbuf.SetDocument(vv, path[nextField+1:], reqValue)
			fbuf.Replace(path[nextField], vv)
			return NewDocumentValue(fbuf), err
		}

		return NewDocumentValue(fbuf), nil
	case ArrayValue:
		buf, err := NewValueBufferByCopy(v)
		vv, index, err := IndexValidator(path, buf)
		if err != nil {
			return v, err
		}

		if path.isOneNestedField() {
			buf.Replace(index, reqValue)
			return NewArrayValue(buf), err
		}

		nextIndex := 1
		if last > 1 {
			nextIndex++
		}

		vv, err = buf.SetArray(vv, path[nextIndex:], reqValue)
		if err != nil {
			return NewArrayValue(buf), err
		}

		buf.Replace(index, vv)
		return NewArrayValue(buf), nil
	default:
		fmt.Println("ReplaceFieldValue: Default:")
		return reqValue, nil
	}
}

// SetUniqueFieldOfDocument Add/Replace a value if the request is <Document.Field> or <Array.Index> or field
func (fb *FieldBuffer) SetUniqueFieldOfDocument(field string, reqValue Value) error {
	_, err := fb.GetByField(field)
	switch err {
	case ErrFieldNotFound:
		fb.Add(field, reqValue)
		fmt.Printf("SetUniqueFieldOfDocument: ErrFieldNotFound: Final value  = %v\n", NewDocumentValue(fb))
		return nil
	case nil:
		fb.Replace(field, reqValue)
		return nil
	default:
		return err
	}
}

// Set replaces a field if it already exists or creates one if not.
func (fb *FieldBuffer) Set(path ValuePath, reqValue Value) error {
	//check the dot notation
	fmt.Printf("***************************** \nSet: reqValue %v with path = %s and len == %d\n **********************\n", reqValue, path, len(path))

	if path.IsUniqueField() {
		return fb.SetUniqueFieldOfDocument(path[0], reqValue)
	}

	for i, field := range fb.fields {
		if path[0] == field.Field {

			fb.fields[i].Value = reqValue
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
