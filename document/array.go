package document

import (
	"github.com/buger/jsonparser"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/types"
)

// ErrValueNotFound must be returned by Array implementations, when calling the GetByIndex method and
// the index wasn't found in the array.
var (
	ErrValueNotFound = errors.New("value not found")
)

// ArrayLength returns the length of an array.
func ArrayLength(a types.Array) (int, error) {
	if vb, ok := a.(*ValueBuffer); ok {
		return len(vb.Values), nil
	}

	var len int
	err := a.Iterate(func(_ int, _ types.Value) error {
		len++
		return nil
	})
	return len, err
}

var errStop = errors.New("stop")

// ArrayContains iterates over a and returns whether v is equal to one of its values.
func ArrayContains(a types.Array, v types.Value) (bool, error) {
	var found bool

	err := a.Iterate(func(i int, vv types.Value) error {
		ok, err := types.IsEqual(vv, v)
		if err != nil {
			return err
		}
		if ok {
			found = true
			return errStop
		}

		return nil
	})

	if err != nil && !errors.Is(err, errStop) {
		return false, err
	}

	return found, nil
}

// ValueBuffer is an array that holds values in memory.
type ValueBuffer struct {
	Values []types.Value
	err    error
}

// NewValueBuffer creates a buffer of values.
func NewValueBuffer(values ...types.Value) *ValueBuffer {
	return &ValueBuffer{Values: values}
}

// Iterate over all the values of the buffer. It implements the Array interface.
func (vb *ValueBuffer) Iterate(fn func(i int, value types.Value) error) error {
	for i, v := range vb.Values {
		err := fn(i, v)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetByIndex returns a value set at the given index. If the index is out of range it returns an error.
func (vb *ValueBuffer) GetByIndex(i int) (types.Value, error) {
	if i >= len(vb.Values) {
		return nil, ErrFieldNotFound
	}

	return vb.Values[i], nil
}

// Len returns the length the of array
func (vb *ValueBuffer) Len() int {
	if vb == nil {
		return 0
	}

	return len(vb.Values)
}

// Append a value to the buffer and return a new buffer.
func (vb *ValueBuffer) Append(v types.Value) *ValueBuffer {
	vb.Values = append(vb.Values, v)
	return vb
}

// ScanArray copies all the values of a to the buffer.
func (vb *ValueBuffer) ScanArray(a types.Array) error {
	return a.Iterate(func(i int, v types.Value) error {
		vb.Values = append(vb.Values, v)
		return nil
	})
}

// Copy deep copies all the values from the given array.
// If a value is a document or an array, it will be stored as a *FieldBuffer or *ValueBuffer respectively.
func (vb *ValueBuffer) Copy(a types.Array) error {
	err := vb.ScanArray(a)
	if err != nil {
		return err
	}

	if len(vb.Values) == 0 {
		return nil
	}

	for i, v := range vb.Values {
		switch v.Type() {
		case types.DocumentValue:
			var buf FieldBuffer
			err = buf.Copy(v.V().(types.Document))
			if err != nil {
				return err
			}

			err = vb.Replace(i, types.NewDocumentValue(&buf))
			if err != nil {
				return err
			}
		case types.ArrayValue:
			var buf ValueBuffer
			err = buf.Copy(v.V().(types.Array))
			if err != nil {
				return err
			}

			err = vb.Replace(i, types.NewArrayValue(&buf))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Apply a function to all the values of the buffer.
func (vb *ValueBuffer) Apply(fn func(p Path, v types.Value) (types.Value, error)) error {
	path := Path{PathFragment{}}

	for i, v := range vb.Values {
		path[0].ArrayIndex = i

		switch v.Type() {
		case types.DocumentValue:
			buf, ok := v.V().(*FieldBuffer)
			if !ok {
				buf = NewFieldBuffer()
				err := buf.Copy(v.V().(types.Document))
				if err != nil {
					return err
				}
			}

			err := buf.Apply(func(p Path, v types.Value) (types.Value, error) {
				return fn(append(path, p...), v)
			})
			if err != nil {
				return err
			}
			vb.Values[i] = types.NewDocumentValue(buf)
		case types.ArrayValue:
			buf, ok := v.V().(*ValueBuffer)
			if !ok {
				buf = NewValueBuffer()
				err := buf.Copy(v.V().(types.Array))
				if err != nil {
					return err
				}
			}

			err := buf.Apply(func(p Path, v types.Value) (types.Value, error) {
				return fn(append(path, p...), v)
			})
			if err != nil {
				return err
			}
			vb.Values[i] = types.NewArrayValue(buf)
		default:
			var err error
			v, err = fn(path, v)
			if err != nil {
				return err
			}
			vb.Values[i] = v
		}
	}

	return nil
}

// Replace the value of the index by v.
func (vb *ValueBuffer) Replace(index int, v types.Value) error {
	if len(vb.Values) <= index {
		return ErrFieldNotFound
	}

	vb.Values[index] = v
	return nil
}

// MarshalJSON implements the json.Marshaler interface.
func (vb ValueBuffer) MarshalJSON() ([]byte, error) {
	return MarshalJSONArray(&vb)
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (vb *ValueBuffer) UnmarshalJSON(data []byte) error {
	var err error
	_, perr := jsonparser.ArrayEach(data, func(value []byte, dataType jsonparser.ValueType, offset int, _ error) {
		v, err := parseJSONValue(dataType, value)
		if err != nil {
			return
		}

		vb.Values = append(vb.Values, v)
	})
	if err != nil {
		return err
	}
	if perr != nil {
		return perr
	}

	return nil
}
