package document

import (
	"errors"
	"fmt"
	"sort"
)

// ErrValueNotFound must be returned by Array implementations, when calling the GetByIndex method and
// the index wasn't found in the array.
var ErrValueNotFound = errors.New("value not found")

// An Array contains a set of values.
type Array interface {
	// Iterate goes through all the values of the array and calls the given function by passing each one of them.
	// If the given function returns an error, the iteration stops.
	Iterate(fn func(i int, value Value) error) error
	// GetByIndex returns a value by index of the array.
	GetByIndex(i int) (Value, error)
}

// ArrayLength returns the length of an array.
func ArrayLength(a Array) (int, error) {
	if vb, ok := a.(ValueBuffer); ok {
		return len(vb), nil
	}

	var len int
	err := a.Iterate(func(_ int, _ Value) error {
		len++
		return nil
	})
	return len, err
}

// ArrayContains iterates over a and returns whether v is equal to one of its values.
func ArrayContains(a Array, v Value) (bool, error) {
	var found bool

	err := a.Iterate(func(i int, vv Value) error {
		ok, err := vv.IsEqual(v)
		if err != nil {
			return err
		}
		if ok {
			found = true
			return errStop
		}

		return nil
	})

	if err != nil && err != errStop {
		return false, err
	}

	return found, nil
}

// ValueBuffer is an array that holds values in memory.
type ValueBuffer []Value

// NewValueBuffer creates a buffer of values.
func NewValueBuffer(values ...Value) ValueBuffer {
	return ValueBuffer(values)
}

// Iterate over all the values of the buffer. It implements the Array interface.
func (vb ValueBuffer) Iterate(fn func(i int, value Value) error) error {
	for i, v := range vb {
		err := fn(i, v)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetByIndex returns a value set at the given index. If the index is out of range it returns an error.
func (vb ValueBuffer) GetByIndex(i int) (Value, error) {
	if i >= len(vb) {
		return Value{}, ErrValueNotFound
	}

	return vb[i], nil
}

// Append a value to the buffer and return a new buffer.
func (vb ValueBuffer) Append(v Value) ValueBuffer {
	return append(vb, v)
}

// ScanArray copies all the values of a to the buffer.
func (vb *ValueBuffer) ScanArray(a Array) error {
	return a.Iterate(func(i int, v Value) error {
		*vb = append(*vb, v)
		return nil
	})
}

// SetValueFromPath set
func (vb *ValueBuffer) SetValueFromPath(path ValuePath, reqValue Value) error {
	index, err := path.findIndexInPath()
	if err != nil {
		return err
	}

	return vb.Replace(index, reqValue)
}

// NewValueBufferByCopy return pointer of ValueBuffer from Value after copying it.
func NewValueBufferByCopy(value Value) (*ValueBuffer, error) {
	if value.Type != ArrayValue {
		return nil, fmt.Errorf("Cannot Create ValueBuffer with type %s\n", value.Type)
	}

	var buf ValueBuffer
	err := buf.Copy(value.V.(Array))
	if err != nil {
		return nil, err
	}

	return &buf, nil
}

// SetArray set value at index
func (vb *ValueBuffer) SetArray(v Value, path ValuePath, reqValue Value) (Value, error) {
	last := path.lastIndexOfPath()
	_, index, _ := IndexValidator(path, vb)

	fmt.Printf("ArrayReplaceValue: path ==  %s, index = %d last == %d\n", path, index, last)
	fmt.Printf("ArrayReplaceValue: V := %v and  V.Type == %s \n", v, v.Type)
	switch v.Type {
	case DocumentValue:
		fmt.Printf("ArrayReplaceValue: DocumentValue: V := %v and  V.Type == %s \n", v, v.Type)
		var buf FieldBuffer
		idx := 0
		_ = buf.Copy(v.V.(Document))
		v, _ := buf.GetByField(path[idx])

		vv, _ := buf.SetDocument(v, path[1:], reqValue)
		buf.Replace(path[0], vv)

		return NewDocumentValue(buf), nil
	case ArrayValue:
		var buf ValueBuffer
		_ = buf.Copy(v.V.(Array))
		vv, index, err := IndexValidator(path, buf)
		if err != nil {
			return NewArrayValue(buf), err
		}

		nextIndex := 1
		if last > 1 {
			va, _ := buf.SetArray(vv, path[nextIndex+1:], reqValue)
			buf.Replace(index, va)
		} else if last == 0 {
			buf.Replace(index, reqValue)
		} else {
			va, _ := buf.SetArray(vv, path[nextIndex:], reqValue)
			buf.Replace(index, va)
		}

		return NewArrayValue(buf), nil
	default:
		return reqValue, nil

	}
}

// Copy deep copies all the values from the given array.
// If a value is a document or an array, it will be stored as a FieldBuffer or ValueBuffer respectively.
func (vb *ValueBuffer) Copy(a Array) error {
	err := vb.ScanArray(a)
	if err != nil {
		return err
	}

	for i, v := range *vb {
		switch v.Type {
		case DocumentValue:
			var buf FieldBuffer
			err = buf.Copy(v.V.(Document))
			if err != nil {
				return err
			}

			_ = vb.Replace(i, NewDocumentValue(&buf))
		case ArrayValue:
			var buf ValueBuffer
			err = buf.Copy(v.V.(Array))
			if err != nil {
				return err
			}

			_ = vb.Replace(i, NewArrayValue(&buf))
		}
	}

	return nil
}

// Replace the value of the index by v.
func (vb *ValueBuffer) Replace(index int, v Value) error {
	if len(*vb) <= index {
		return ErrIndexOutOfBound
	}

	(*vb)[index] = v
	return nil
}

type sortableArray struct {
	vb  ValueBuffer
	err error
}

func (a sortableArray) Len() int {
	return len(a.vb)
}

func (a *sortableArray) Swap(i, j int) { a.vb[i], a.vb[j] = a.vb[j], a.vb[i] }

var typeSortOrder = map[ValueType]int{
	NullValue:     0,
	BoolValue:     1,
	Float64Value:  2,
	TextValue:     3,
	ArrayValue:    4,
	DocumentValue: 5,
}

func (a *sortableArray) Less(i, j int) (ok bool) {
	it, jt := a.vb[i].Type, a.vb[j].Type
	if it == jt {
		ok, a.err = a.vb[i].IsLesserThan(a.vb[j])
		return
	}

	switch {
	case it.IsNumber():
		it = Float64Value
	case it == BlobValue:
		it = TextValue
	}

	switch {
	case jt.IsNumber():
		jt = Float64Value
	case jt == BlobValue:
		jt = TextValue
	}

	if typeSortOrder[it] == typeSortOrder[jt] {
		ok, a.err = a.vb[i].IsLesserThan(a.vb[j])
		return
	}

	return typeSortOrder[it]-typeSortOrder[jt] < 0
}

// SortArray creates a new sorted array.
// Types are sorted in the following ascending order:
//   - NULL
//   - Booleans
//   - Numbers
//   - Text / Blob
//   - Arrays
//   - Documents
// It doesn't sort nested arrays.
func SortArray(a Array) (Array, error) {
	var s sortableArray
	err := s.vb.ScanArray(a)
	if err != nil {
		return nil, err
	}

	sort.Sort(&s)

	if s.err != nil {
		return nil, err
	}

	return &s.vb, nil
}
