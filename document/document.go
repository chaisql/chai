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

func setArrayValue(value Value, index int, reqValue Value) (ValueBuffer, error) {
	array, _ := value.ConvertToArray()
	var vbuf ValueBuffer

	err := array.Iterate(func(i int, va Value) error {
		fmt.Printf("ITER: i := %d and value va := %s & index %d\n", i, va, index)
		if i == index {
			vbuf = vbuf.Append(reqValue)
		} else {
			vbuf = vbuf.Append(va)
		}
		return nil
	})
	if err != nil {
		return vbuf, err
	}

	return vbuf, nil
}

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

// FieldValidator iterate over the path
func FieldValidator(d Document, path ValuePath) error {
	last := path.lastIndexOfPath()
	if last == 1 || len(path) == 0 {
		return ErrValueNotSet
	}

	for i, p := range path {
		v, err := d.GetByField(p)
		if err != nil {
			if i == last {
				return ErrCreateField
			}
			return ErrFieldNotFound
		}

		if v.Type == ArrayValue {
			arr, _ := v.ConvertToArray()
			_, err := IndexValidator(path[i+1:], arr)
			return err
		}
	}

	return nil
}

// IndexValidator check if the index is not out of range
func IndexValidator(path ValuePath, a Array) (int, error) {

	index, err := path.findIndexInPath()
	if err != nil {
		return -1, err
	}

	_, err = a.GetByIndex(index)
	if err != nil {
		fmt.Printf("index validator Err := %s\n", err)
		return index, ErrIndexOutOfBound
	}

	return index, nil
}

//
func setDocumentValue(value Value, f string, reqValue Value) (Value, error) {
	d, err := value.ConvertToDocument()
	if err != nil {
		return value, err
	}

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
	case ArrayValue:
		var buf ValueBuffer
		err := buf.Copy(v.V.(Array))
		if err != nil {
			return v, err
		}

		err = buf.ArrayReplaceValue(path[1:], reqValue)
		if err != nil {
			return v, err
		}

		return NewArrayValue(buf), nil
	}

	return v, errors.New("type must be an array or a document")
}

//ReplaceFieldValue reur
func (fb *FieldBuffer) ReplaceFieldValue(path ValuePath, reqValue Value) error {
	last := path.lastIndexOfPath()
	for i, f := range fb.fields {
		if f.Field == path[0] {
			switch f.Value.Type {
			case DocumentValue:
				var fbuf FieldBuffer
				err := fbuf.Copy(f.Value.V.(Document))
				if err != nil {
					return err
				}

				v, err := fbuf.GetByField(path[1])
				if err != nil {
					return fb.ReplaceFieldValue(path[1:], reqValue)
				}

				if last == 1 {
					va, _ := setDocumentValue(f.Value, path[last], reqValue)
					fb.fields[i].Value = va
					return nil
				}

				v, err = replaceValue(v, path[1:], reqValue)
				fbuf.Replace(path[1], v)
				fb.fields[i].Value = NewDocumentValue(fbuf)
				return nil
			case ArrayValue:
				var buf ValueBuffer
				arr, _ := f.Value.ConvertToArray()
				err := buf.Copy(arr)
				if err != nil {
					return err
				}

				err = buf.ArrayReplaceValue(path[1:], reqValue)
				if err != nil {
					return err
				}

				fb.fields[i].Value = NewArrayValue(buf)
				return nil
			default:
				fb.fields[i].Value = reqValue
				return nil
			}
		}
	}
	fb.Add(path[0], reqValue)
	return nil
}

// Set replaces a field if it already exists or creates one if not.
func (fb *FieldBuffer) Set(pa ValuePath, value Value) error {
	//check the dot notation
	for i, field := range fb.fields {
		if pa[0] != field.Field {
			continue
		}
		switch field.Value.Type {
		case DocumentValue:
			var fbuf FieldBuffer
			err := fbuf.Copy(field.Value.V.(Document))
			if err != nil {
				return err
			}

			fbuf.ReplaceFieldValue(pa[1:], value)
			fb.fields[i].Value = NewDocumentValue(fbuf)
			return nil
		case ArrayValue:
			var buf ValueBuffer
			err := buf.Copy(field.Value.V.(Array))
			if err != nil {
				return err
			}

			err = buf.ArrayReplaceValue(pa[1:], value)
			if err != nil {
				return err
			}

			fb.Replace(field.Field, NewArrayValue(buf))
			fmt.Printf("Set: final Value:: fb.fields[i] == %v\n", fb.fields[i].Value)
			return nil
		default:
			fb.Replace(field.Field, value)
			return nil
		}
	}

	fb.Add(pa[0], value)
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
