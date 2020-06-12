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

/*setArrayValue update the value of array at the given index.
func (p ValuePath) setArrayValue(vlist Array, v Value, index int) (Value, error) {
	var buf ValueBuffer
	fmt.Printf("value set Array %v\n", v)
	index, errConv := strconv.Atoi(p)
	if errConv != nil {
		index = ipath
	}
	err := vlist.Iterate(func(i int, va Value) error {
		fmt.Printf("set Array va TYPE %v\n", va.Type)
		if va.Type == DocumentValue {

		}
		if index == i {
			buf = buf.Append(v)
			return nil
		}
		buf = buf.Append(va)
		return nil
	})

	if err == nil {
		vb := NewArrayValue(buf)
		return vb, err
	}

	return NewNullValue(), err
}*/

var (
	errShortNotation = errors.New("Short Notation")
)

/* SetDotNotation allow dot notation and replace value at the given index.
func (fb *FieldBuffer) SetDotNotation(fname ValuePath, value Value) error {
	fmt.Printf("vPath %s\n", fname)
	if len(fname) == 1 {
		return errShortNotation
	}

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
			for ipath, fpath := range fname {
				fmt.Printf("ipath %d && fpath %s\n", ipath, fpath)
				v, err := d.GetByField(fpath)
				fmt.Printf("v TYPE %s\n", v.Type)
				if err == nil {
					fmt.Println(err)
					doc, errDoc := v.ConvertToDocument()
					if errDoc != nil {
						if v.Type == ArrayValue {
							fmt.Println("v.Type ", v.Type, v)
							varr, _ := v.ConvertToArray()

							vFromArr, _ := fname.setArrayValue(varr, value, index)
							buf.Delete(fpath)
							buf.Add(fpath, vFromArr)
							continue
						} else {
							buf.Replace(fpath, value)
						}

					} else {
						b1, errDoc := fname.setDocumentValue(doc, value)
						if errDoc == nil {
							buf.Delete(fpath)
							valueF := NewDocumentValue(b1)
							buf.Add(fpath, valueF)
							fmt.Printf("buf := %v\n", buf)
						} else {
							return errDoc
						}
					}
				}
			}
			fb.fields[i].Value = NewDocumentValue(&buf)
		case ArrayValue:
			var buf ValueBuffer
			var err error
			vlist, _ := f.Value.ConvertToArray()
			fmt.Printf("Value %v && size of array := %d and fname := %s\n", f.Value, len(fname), fname[1])
			//the position of the index (fieldname.index)
			for idx := 1; idx < len(fname); idx++ {
				fmt.Printf("i := %d && fname[%d] = %s\n", idx, idx, fname[idx])
				index, ErrConv := strconv.Atoi(fname[idx])
				if ErrConv != nil {
					fmt.Println(ErrConv)
					err = ErrConv
					break
				}
				buf, errArray := fname.setArrayValue(vlist, value, index)

				if errArray == nil {
					fmt.Printf("fb.fields[i].Value %v\n", fb.fields[i].Value)
					fb.fields[i].Value = NewArrayValue(&buf)
					return nil
				}
			}
			if err != nil {
				fb.fields[i].Value = NewArrayValue(&buf)
			}
		}
	}
	return nil
}*/

func setArrayValue(f string, toSet bool) {

}

func (fb *FieldBuffer) setDocumentValue(value Value, f string, reqValue Value, index int) (Value, error) {
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
	if err != nil {
		return NewZeroValue(DocumentValue), err
	}

	return NewDocumentValue(fbuf), nil
}

// SetUpdate
func (fb *FieldBuffer) SetUpdate(p ValuePath, value Value, t ValueType, index int) error {
	last := len(p) - 1
	var fbuf FieldBuffer
	err := fbuf.Copy(fb.fields[index].Value.V.(Document))
	if err != nil {
		return err
	}
	fmt.Println("in Value := ", fb.fields[index].Value)
	d, err := fb.fields[index].Value.ConvertToDocument()
	if err != nil {
		fmt.Println(err)
		return err
	}

	for i := 1; i < last; i++ {
		v, err := d.GetByField(p[i])
		if err != nil {
			return err
		}
		switch v.Type {
		case DocumentValue:
			v, err = fb.setDocumentValue(v, p[last], value, index)
			if err != nil {
				fmt.Println(err)
				return err
			}
			fbuf.Delete(p[i])
			fbuf.Add(p[i], v)
		case ArrayValue:

		}

	}
	switch fb.fields[index].Value.Type {
	case DocumentValue:
		fb.fields[index].Value = NewDocumentValue(fbuf)
	}
	/*var vbuf ValueBuffer
	for i, p := range vpath {
		if i == last {
			setChange = true
		}
		//setArrayValue(p, setChange)
	}*/

	return nil
}

// Set replaces a field if it already exists or creates one if not.
func (fb *FieldBuffer) Set(p ValuePath, value Value) error {
	//check the dot notation
	for i, field := range fb.fields {
		if field.Field != p[0] {
			continue
		}
		switch field.Value.Type {
		case DocumentValue:
			fmt.Printf("i of frang %d && field %v\n", i, fb.fields[i].Value)
			fmt.Printf("Document == f.Value.Type %v and field.Value %v and field Name: %s\n", field.Value.Type, field.Value, field.Field)
			fb.SetUpdate(p, value, field.Value.Type, i)
		case ArrayValue:

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
