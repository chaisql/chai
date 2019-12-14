package document

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
)

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

type ValueBuffer []Value

func NewValueBuffer() ValueBuffer {
	return ValueBuffer{}
}

func (vb ValueBuffer) Iterate(fn func(i int, value Value) error) error {
	for i, v := range vb {
		err := fn(i, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (vb ValueBuffer) GetByIndex(i int) (Value, error) {
	if i >= len(vb) {
		return Value{}, fmt.Errorf("value at index %d not found", i)
	}

	return vb[i], nil
}

func (vb ValueBuffer) Append(v Value) ValueBuffer {
	return append(vb, v)
}

func (vb *ValueBuffer) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))

	t, err := dec.Token()
	if err == io.EOF {
		return err
	}

	// expecting a '['
	if d, ok := t.(json.Delim); !ok || d.String() != "[" {
		return fmt.Errorf("found %q, expected '['", d.String())
	}

	for dec.More() {
		v, err := parseJSONValue(dec)
		if err != nil {
			return err
		}

		*vb = vb.Append(v)
	}

	t, err = dec.Token()
	if err == io.EOF {
		return err
	}

	// expecting a ']'
	if d, ok := t.(json.Delim); !ok || d.String() != "]" {
		return fmt.Errorf("found %q, expected ']'", d.String())
	}

	return nil
}

type sliceArray struct {
	ref reflect.Value
}

var _ Array = (*sliceArray)(nil)

func (s sliceArray) Iterate(fn func(i int, v Value) error) error {
	l := s.ref.Len()

	for i := 0; i < l; i++ {
		f := s.ref.Index(i)

		v, err := reflectValueToValue(f)
		if err == errUnsupportedType {
			continue
		}
		if err != nil {
			return err
		}

		err = fn(i, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s sliceArray) GetByIndex(i int) (Value, error) {
	if i >= s.ref.Len() {
		return Value{}, ErrFieldNotFound
	}

	v := s.ref.Index(i)
	if !v.IsValid() {
		return Value{}, ErrFieldNotFound
	}

	return reflectValueToValue(v)
}
