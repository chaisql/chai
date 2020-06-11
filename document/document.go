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

// setArrayValue update the value of array at the given index.
func setArrayValue(vlist Array, v Value, index int) (ValueBuffer, error) {
	var buf ValueBuffer
	err := vlist.Iterate(func(i int, va Value) error {
		if index == i {
			buf = buf.Append(v)
			return nil
		}
		buf = buf.Append(va)
		return nil
	})

	return buf, err
}

func setDocumentValue(d Document, value Value, p ValuePath) (FieldBuffer, error) {
	var buf FieldBuffer
	index := len(p) - 1
	fmt.Printf("func: set Doc Value == %v\n", value)

	err := d.Iterate(func(field string, va Value) error {
		fmt.Printf("Iterate => field == %s Value == %v\n", field, va)
		if va.Type == DocumentValue {
			v, err := d.GetByField(field)
			if err != nil {
				return err
			}
			fmt.Printf("Iterate => v == %v\n", v)
		}
		if p[index] == field {
			fmt.Printf("path[index] == %s and value %v\n", p[index], value)
			buf.Add(field, value)
		} else {
			fmt.Printf("Add field == %s and va %v\n", field, va)
			buf.Add(field, va)
		}
		return nil
	})

	return buf, err
}

// SetDotNotation allow dot notation and replace value at the given index.
func (fb *FieldBuffer) SetDotNotation(fname ValuePath, value Value) error {

	for i, f := range fb.fields {

		if f.Field != fname[0] {
			continue
		}
		switch f.Value.Type {
		case DocumentValue:
			var buf FieldBuffer
			d, _ := f.Value.ConvertToDocument()
			err := buf.Copy(f.Value.V.(Document))
			if err != nil {
				return err
			}
			//May be another way to do it
			for _, fpath := range fname {
				v, err := d.GetByField(fpath)
				if err == nil {
					fmt.Println(err)
					doc, _ := v.ConvertToDocument()
					b1, errDoc := setDocumentValue(doc, value, fname)
					if errDoc == nil {
						buf.Delete(fpath)
						valueF := NewDocumentValue(b1)
						buf.Add(fpath, valueF)
					}
				}

			}
			fb.fields[i].Value = NewDocumentValue(&buf)
		case ArrayValue:
			vlist, _ := f.Value.ConvertToArray()
			fmt.Println(vlist)
			//the position of the index (fieldname.index)
			/*buf, err := setArrayValue(vlist, v, index)
			if err == nil {
				fb.fields[i].Value = NewArrayValue(&buf)
				return err
			}*/
		}
	}

	/*INSERT INTO client (prenom, nom, ville, age)
	// contact: { phone: { type: "cell", number: "111-222-3333" } }
	*/
	return nil
}

// Set replaces a field if it already exists or creates one if not.
func (fb *FieldBuffer) Set(f ValuePath, v Value) error {
	//check the dot notation
	err := fb.SetDotNotation(f, v)
	if err != nil {
		return err
	}
	return nil

	/*for i := range fb.fields {
		if fb.fields[i].Field == f {
			fb.fields[i].Value = v
			return nil
		}
	}*/
	//fb.Add(f, v)

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
	//check if there is a dot notation

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

//isDotPath verify if the path contains a dot
func isDotPath(p string) bool {
	return strings.Contains(p, ".")
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
