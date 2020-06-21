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
	list, _ := value.ConvertToArray()
	var vbuf ValueBuffer

	err := list.Iterate(func(i int, va Value) error {
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
	len := 0

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
			fmt.Printf("find index := %d\n", index)
			return index, nil
		}
	}

	return 0, err
}

// IndexValidator check if the index is not out of range
func IndexValidator(path ValuePath, a Array) (int, error) {
	fmt.Println("In array function")
	size := Lenght(a)
	index, err := path.findIndexInPath()
	if err != nil {
		fmt.Println("Err := ", a)
		return 0, err
	}

	if index >= size {
		fmt.Printf("index %d && size %d\n", index, size)
		fmt.Println("Err := ", a)
		return index, errors.New("index out of bounds")
	}
	return index, nil
}

func setArray(arr Value, path ValuePath, value Value) (Value, error) {
	d, _ := arr.ConvertToArray()
	size := Lenght(d)
	fmt.Printf("in set Array == %v and path == %s and size %d\n", arr, path, size)
	last := len(path) - 1
	var vbuf ValueBuffer
	index, err := IndexValidator(path, d)
	if err != nil {
		fmt.Printf("error index validator == %s\n", err)
		return arr, err
	}
	fmt.Printf("validate index ==  %d\n", index)
	for i := 0; i < size; i++ {
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
		fmt.Println(err)
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
func (fb *FieldBuffer) SetDocument(d Document, path ValuePath, value Value) (FieldBuffer, error) {
	last := len(path) - 1
	var fbuf FieldBuffer

	for i := 0; i < len(path); i++ {
		v, err := d.GetByField(path[i])
		fmt.Printf("in SET DOC %s, i == %d and last == %d\n", v, i, last)
		fmt.Printf("Type := %s and path %s\n", v.Type, path)
		if err != nil {
			return fbuf, err
		}
		switch v.Type {
		case DocumentValue:
			if i == last || last == 1 {
				va, err := setDocumentValue(v, path[last], value)
				if err != nil {
					fmt.Println(err)
					return fbuf, err
				}
				fmt.Printf("REPLACE \n")
				fbuf.Add(path[i], va)
				return fbuf, nil
			}
			fmt.Printf("RECURS \n")
			buf, _ := fb.SetDocument(d, path[i+1:], value)
			vf := NewDocumentValue(buf)
			fbuf.Add(path[i], vf)
			fmt.Printf("buf %v\n", buf)

		case ArrayValue:
			fmt.Printf("Array Value  p := %s\n", path[i:])
			va, err := setArray(v, path[i:], value)
			if err != nil {
				fmt.Printf("error in array %s\n", err)
				return fbuf, err
			}

			fmt.Printf("Array Value befor AADD index %d and path %s and va %s\n", i, path[i], va)
			fbuf.Add(path[i], va)
			fmt.Printf("fbuf %v\n", fbuf)
			return fbuf, nil
		case TextValue:
			fmt.Printf("in SET DOC VALUE TXT v := %s and value %s and path %s\n", v, value, path[i])
			fbuf.Add(path[i], value)

		}

	}
	return fbuf, nil
}

// Set replaces a field if it already exists or creates one if not.
func (fb *FieldBuffer) Set(p ValuePath, value Value) error {
	//check the dot notation
	for _, field := range fb.fields {
		if p[0] != field.Field {
			continue
		}
		switch field.Value.Type {
		case DocumentValue:
			d, err := field.Value.ConvertToDocument()
			if err != nil {
				fmt.Println(err)
				return err
			}
			fbuf, err := fb.SetDocument(d, p[1:], value)
			if err != nil {
				fmt.Printf("return error =>> %s\n", err)
				return err
			}
			vf := NewDocumentValue(fbuf)
			fb.Replace(field.Field, vf)
			return nil
		case ArrayValue:
			x := field.Value
			arr, _ := x.ConvertToArray()
			_, err := IndexValidator(p[1:], arr)
			if err != nil {
				fb.Replace(field.Field, x)
				return errors.New("out of range")
			}
			va, _ := setArray(field.Value, p[1:], value)
			fmt.Println("va == ", va, va.Type)
			fb.Replace(field.Field, va)
			return nil
		default:
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
		fmt.Printf("fb.fields[i].Field ==  %s\n", fb.fields[i].Field)
		if fb.fields[i].Field == field {
			fmt.Printf("field := %s and value %v\n", field, v)
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
