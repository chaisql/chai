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

// Lenght return size of array
func Lenght(a Array) int {
	len := 1

	_ = a.Iterate(func(i int, va Value) error {
		len++
		return nil
	})
	return len
}

// ArrayFindIndex
func (path ValuePath) findIndexInPath() (int, error) {
	var err error
	for _, p := range path {
		index, err := strconv.Atoi(p)
		if err == nil {
			return index, nil
		}
	}

	return 0, err
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
	size := Lenght(a)
	index, err := path.findIndexInPath()
	if err != nil {
		fmt.Println("Err := ", a)
		return 0, err
	}

	if index >= size {
		fmt.Printf("index %d && size %d\n", index, size)
		return index, ErrIndexOutOfBound
	}

	return index, nil
}

func setArray(arr Value, path ValuePath, value Value) (Value, error) {
	d, _ := arr.ConvertToArray()
	size := Lenght(d)
	fmt.Printf("in set Array == %v and path == %s and size %d\n", arr, path, size)
	last := path.lastIndexOfPath()
	var vbuf ValueBuffer
	index, err := IndexValidator(path, d)
	if err != nil {
		fmt.Printf("error in array %s\n", err)
		return arr, err
	}
	fmt.Printf("validate index ==  %d\n", index)
	for i := 0; i < (size - 1); i++ {
		fmt.Printf("i == %d idx %d and size %d\n", i, index, size)
		v, err := d.GetByIndex(i)
		if err != nil {
			fmt.Printf("error get index == %s\n", err)
			return arr, err
		}
		fmt.Printf("v by index %v\n", v)
		switch v.Type {
		case DocumentValue:
			if i == index {
				va, err := setDocumentValue(v, path[last], value)
				fmt.Println("VA ", va, err)
				if err != nil {
					fmt.Println(err)
					return NewArrayValue(vbuf), err
				}
				vbuf = vbuf.Append(va)
			} else {
				fmt.Println("V == ", v)
				vbuf = vbuf.Append(v)
			}
		case ArrayValue:
			vf, err := setArrayValue(arr, index, value)
			if err != nil {
				return arr, err
			}
			fmt.Println("VF ", vf)
			return NewArrayValue(vf), nil
		default:
			fmt.Printf("Set array value at index ==  %d\n", index)
			vf, _ := setArrayValue(arr, index, value)
			fmt.Println("VF ", vf)
			return NewArrayValue(vf), nil
		}
	}

	return NewArrayValue(vbuf), nil
}

func setDocumentValue(value Value, f string, reqValue Value) (Value, error) {
	fmt.Printf("field in req %s and Value in req %v\n", f, reqValue)
	d, err := value.ConvertToDocument()
	if err != nil {
		fmt.Printf("error to convert %s\n", err)
		return NewZeroValue(DocumentValue), err
	}

	var fbuf FieldBuffer
	err = d.Iterate(func(field string, va Value) error {
		if f == field {
			fmt.Printf("field %s and Value %v\n", field, reqValue)
			fbuf.Add(field, reqValue)
		} else {
			fmt.Printf("else field %s and Value %v\n", field, va)
			fbuf.Add(field, va)
		}
		return nil
	})
	_, errField := d.GetByField(f)
	if errField != nil {
		fbuf.Add(f, reqValue)
	}
	return NewDocumentValue(fbuf), nil
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

// SetDocument set a document
func (fb *FieldBuffer) SetDocument(d Document, path ValuePath, value Value) (Value, error) {
	last := len(path) - 1
	var fbuf FieldBuffer

	for i := 0; i < len(path); i++ {
		v, err := d.GetByField(path[i])
		fmt.Printf("in SET DOC %s, i == %d and last == %d\n", v, i, last)
		fmt.Printf("Type := %s and path %s and value %v\n", v.Type, path[i], v)
		if err != nil {
			if i == last {
				fbuf.Copy(d)
				fbuf.Add(path[i], value)
				fmt.Printf("Type := %s and path %s and value %v\n", v.Type, path, NewDocumentValue(fbuf))
				return NewDocumentValue(fbuf), ErrCreateField
			}
			return NewZeroValue(DocumentValue), err
		}
		switch v.Type {
		case DocumentValue:
			if i == last || last == 1 {
				v, err := setDocumentValue(v, path[last], value)
				if err != nil {
					fmt.Println(err)
					return v, err
				}
				fbuf.Add(path[i], v)
				fmt.Printf("return va %v\n", v)
				return v, nil
			}
			fmt.Printf("RECURS %s\n", path[i+1:])
			buf, _ := fb.SetDocument(d, path[i+1:], value)

			return buf, nil
		case ArrayValue:
			fmt.Printf("Array Value  p := %s\n", path[i:])
			v, err := setArray(v, path[i:], value)
			if err != nil {
				fmt.Printf("error in array %s\n", err)
				return v, err
			}
			fmt.Printf("Array Value befor AADD and va type %s and va %s\n", v.Type, v)

			return v, nil
		case TextValue:
			return value, nil
		}

	}
	return NewZeroValue(DocumentValue), nil
}

// Set replaces a field if it already exists or creates one if not.
func (fb *FieldBuffer) Set(p ValuePath, value Value) error {
	//check the dot notation
	for i, field := range fb.fields {
		fmt.Printf("field == %s and fb[i] :: %v\n", field.Field, fb.fields[i].Value)
		if p[0] != field.Field && field.Value.Type != ArrayValue {
			continue
		}
		switch field.Value.Type {
		case DocumentValue:
			d, err := field.Value.ConvertToDocument()
			va, err := fb.SetDocument(d, p[1:], value)

			switch err {
			case ErrCreateField:
				fb.Replace(field.Field, va)
				return err
			case ErrIndexOutOfBound:
				return err
			}

			//set the first node of the document
			vv, _ := setDocumentValue(field.Value, p[1], va)
			fmt.Printf("vv == %v\n", vv)
			fb.Replace(field.Field, vv)
			return nil
		case ArrayValue:
			fmt.Printf("p size %d\n", len(p))
			if len(p) == 1 {
				var buf FieldBuffer
				buf.Add(field.Field, field.Value)
				buf.Add(p[0], value)
				v := NewDocumentValue(buf)
				fmt.Printf("value of array %v\n", v)
				fb.fields[i].Value = v

				return nil
			}
			arr, err := field.Value.ConvertToArray()
			_, err = IndexValidator(p[1:], arr)
			fmt.Printf("error in before if array set %s\n", err)
			if err != nil {
				return errors.New("out of range")
			}

			va, _ := setArray(field.Value, p[1:], value)
			vv, _ := setDocumentValue(field.Value, p[1], va)
			fmt.Printf("vv == %v\n", vv)
			fb.Replace(field.Field, va)
			return nil
		default:
			fmt.Printf("field == %s && value == %v and type %s\n", field.Field, value, value.Type)
			fb.Replace(field.Field, value)
			return nil
		}
	}

	fb.Add(p[0], value)
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
		fmt.Printf("array to change %v\n", fb.fields[i].Value)
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
