package document

import (
	"errors"
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

// Copy deep copies all the values from the given array.
// If a value is a document or an array, it will be stored as a FieldBuffer or ValueBuffer respectively.
func (vb *ValueBuffer) Copy(a Array) error {
	err := vb.ScanArray(a)
	if err != nil {
		return err
	}

	for _, v := range *vb {
		switch v.Type {
		case DocumentValue:
			var buf FieldBuffer
			err = buf.Copy(v.V.(Document))
			if err != nil {
				return err
			}

			*vb = vb.Append(NewDocumentValue(&buf))
		case ArrayValue:
			var buf ValueBuffer
			err = buf.Copy(v.V.(Array))
			if err != nil {
				return err
			}

			*vb = vb.Append(NewArrayValue(&buf))
		}
	}

	return nil
}

// Replace the value of the index by v.
func (vb *ValueBuffer) Replace(index int, v Value) error {
	if len(*vb) <= index {
		return ErrFieldNotFound
	}

	(*vb)[index] = v
	return nil
}
