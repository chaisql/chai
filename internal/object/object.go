// Package object defines types to manipulate and compare objects and values.
package object

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/buger/jsonparser"
	"github.com/cockroachdb/errors"

	"github.com/chaisql/chai/internal/stringutil"
	"github.com/chaisql/chai/internal/types"
)

// ErrUnsupportedType is used to skip struct or array fields that are not supported.
type ErrUnsupportedType struct {
	Value interface{}
	Msg   string
}

func (e *ErrUnsupportedType) Error() string {
	return fmt.Sprintf("unsupported type %T. %s", e.Value, e.Msg)
}

// An Iterator can iterate over object keys.
type Iterator interface {
	// Iterate goes through all the objects and calls the given function by passing each one of them.
	// If the given function returns an error, the iteration stops.
	Iterate(fn func(d types.Object) error) error
}

// MarshalJSON encodes an object to json.
func MarshalJSON(d types.Object) ([]byte, error) {
	return types.NewObjectValue(d).MarshalJSON()
}

// MarshalJSONArray encodes an array to json.
func MarshalJSONArray(a types.Array) ([]byte, error) {
	return types.NewArrayValue(a).MarshalJSON()
}

// Length returns the length of an object.
func Length(d types.Object) (int, error) {
	if fb, ok := d.(*FieldBuffer); ok {
		return fb.Len(), nil
	}

	var len int
	err := d.Iterate(func(_ string, _ types.Value) error {
		len++
		return nil
	})
	return len, err
}

// FieldBuffer stores a group of fields in memory. It implements the object interface.
type FieldBuffer struct {
	fields []fieldValue
}

// NewFieldBuffer creates a FieldBuffer.
func NewFieldBuffer() *FieldBuffer {
	return new(FieldBuffer)
}

// MarshalJSON implements the json.Marshaler interface.
func (fb *FieldBuffer) MarshalJSON() ([]byte, error) {
	return MarshalJSON(fb)
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (fb *FieldBuffer) UnmarshalJSON(data []byte) error {
	return jsonparser.ObjectEach(data, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		v, err := parseJSONValue(dataType, value)
		if err != nil {
			return err
		}

		fb.Add(string(key), v)
		return nil
	})
}

func (fb *FieldBuffer) String() string {
	s, _ := fb.MarshalJSON()
	return string(s)
}

type fieldValue struct {
	Field string
	Value types.Value
}

// Add a field to the buffer.
func (fb *FieldBuffer) Add(field string, v types.Value) *FieldBuffer {
	fb.fields = append(fb.fields, fieldValue{field, v})
	return fb
}

// ScanObject copies all the fields of d to the buffer.
func (fb *FieldBuffer) ScanObject(d types.Object) error {
	return d.Iterate(func(f string, v types.Value) error {
		fb.Add(f, v)
		return nil
	})
}

// GetByField returns a value by field. Returns an error if the field doesn't exists.
func (fb FieldBuffer) GetByField(field string) (types.Value, error) {
	for _, fv := range fb.fields {
		if fv.Field == field {
			return fv.Value, nil
		}
	}

	return nil, types.ErrFieldNotFound
}

// setFieldValue replaces a field if it already exists or creates one if not.
func (fb *FieldBuffer) setFieldValue(field string, reqValue types.Value) error {
	_, err := fb.GetByField(field)
	switch err {
	case types.ErrFieldNotFound:
		fb.Add(field, reqValue)
		return nil
	case nil:
		_ = fb.Replace(field, reqValue)
		return nil
	}

	return err
}

// setValueAtPath deep replaces or creates a field at the given path
func setValueAtPath(v types.Value, p Path, newValue types.Value) (types.Value, error) {
	switch v.Type() {
	case types.ObjectValue:
		var buf FieldBuffer
		err := buf.ScanObject(types.As[types.Object](v))
		if err != nil {
			return v, err
		}

		if len(p) == 1 {
			err = buf.setFieldValue(p[0].FieldName, newValue)
			return types.NewObjectValue(&buf), err
		}

		// the field is an object but the path expects an array,
		// return an error
		if p[0].FieldName == "" {
			return nil, types.ErrFieldNotFound
		}

		va, err := buf.GetByField(p[0].FieldName)
		if err != nil {
			return v, err
		}

		va, err = setValueAtPath(va, p[1:], newValue)
		if err != nil {
			return v, err
		}

		err = buf.setFieldValue(p[0].FieldName, va)
		return types.NewObjectValue(&buf), err
	case types.ArrayValue:
		var vb ValueBuffer
		err := vb.ScanArray(types.As[types.Array](v))
		if err != nil {
			return v, err
		}

		va, err := vb.GetByIndex(p[0].ArrayIndex)
		if err != nil {
			return v, err
		}

		if len(p) == 1 {
			err = vb.Replace(p[0].ArrayIndex, newValue)
			return types.NewArrayValue(&vb), err
		}

		va, err = setValueAtPath(va, p[1:], newValue)
		if err != nil {
			return v, err
		}
		err = vb.Replace(p[0].ArrayIndex, va)
		return types.NewArrayValue(&vb), err
	}

	return nil, types.ErrFieldNotFound
}

// Set replaces a field if it already exists or creates one if not.
// TODO(asdine): Set should always fail with types.ErrFieldNotFound if the path
// doesn't resolve to an existing field.
func (fb *FieldBuffer) Set(path Path, v types.Value) error {
	if len(path) == 0 || path[0].FieldName == "" {
		return types.ErrFieldNotFound
	}

	if len(path) == 1 {
		return fb.setFieldValue(path[0].FieldName, v)
	}

	container, err := fb.GetByField(path[0].FieldName)
	if err != nil {
		return err
	}

	va, err := setValueAtPath(container, path[1:], v)
	if err != nil {
		return err
	}

	err = fb.setFieldValue(path[0].FieldName, va)
	if err != nil {
		return err
	}

	return nil
}

// Iterate goes through all the fields of the object and calls the given function by passing each one of them.
// If the given function returns an error, the iteration stops.
func (fb FieldBuffer) Iterate(fn func(field string, value types.Value) error) error {
	for _, fv := range fb.fields {
		err := fn(fv.Field, fv.Value)
		if err != nil {
			return err
		}
	}

	return nil
}

// Delete a field from the buffer.
func (fb *FieldBuffer) Delete(path Path) error {
	if len(path) == 1 {
		for i := range fb.fields {
			if fb.fields[i].Field == path[0].FieldName {
				fb.fields = append(fb.fields[0:i], fb.fields[i+1:]...)
				return nil
			}
		}
	}

	parentPath := path[:len(path)-1]
	lastFragment := path[len(path)-1]

	// get parent doc or array
	v, err := parentPath.GetValueFromObject(fb)
	if err != nil {
		return err
	}
	switch v.Type() {
	case types.ObjectValue:
		subBuf, ok := types.Is[*FieldBuffer](v)
		if !ok {
			return errors.New("delete doesn't support non buffered object")
		}

		for i := range subBuf.fields {
			if subBuf.fields[i].Field == lastFragment.FieldName {
				subBuf.fields = append(subBuf.fields[0:i], subBuf.fields[i+1:]...)
				return nil
			}
		}

		return types.ErrFieldNotFound
	case types.ArrayValue:
		subBuf, ok := types.Is[*ValueBuffer](v)
		if !ok {
			return errors.New("delete doesn't support non buffered array")
		}

		idx := path[len(path)-1].ArrayIndex
		if idx >= len(subBuf.Values) {
			return types.ErrFieldNotFound
		}
		subBuf.Values = append(subBuf.Values[0:idx], subBuf.Values[idx+1:]...)
	default:
		return types.ErrFieldNotFound
	}

	return nil
}

// Replace the value of the field by v.
func (fb *FieldBuffer) Replace(field string, v types.Value) error {
	for i := range fb.fields {
		if fb.fields[i].Field == field {
			fb.fields[i].Value = v
			return nil
		}
	}

	return types.ErrFieldNotFound
}

// Copy deep copies every value of the object to the buffer.
// If a value is an object or an array, it will be stored as a FieldBuffer or ValueBuffer respectively.
func (fb *FieldBuffer) Copy(d types.Object) error {
	return d.Iterate(func(field string, value types.Value) error {
		v, err := CloneValue(value)
		if err != nil {
			return err
		}
		fb.Add(strings.Clone(field), v)
		return nil
	})
}

func CloneValue(v types.Value) (types.Value, error) {
	switch v.Type() {
	case types.NullValue:
		return types.NewNullValue(), nil
	case types.BooleanValue:
		return types.NewBoolValue(types.As[bool](v)), nil
	case types.IntegerValue:
		return types.NewIntegerValue(types.As[int64](v)), nil
	case types.DoubleValue:
		return types.NewDoubleValue(types.As[float64](v)), nil
	case types.TimestampValue:
		return types.NewTimestampValue(types.As[time.Time](v)), nil
	case types.TextValue:
		return types.NewTextValue(strings.Clone(types.As[string](v))), nil
	case types.BlobValue:
		return types.NewBlobValue(append([]byte{}, types.As[[]byte](v)...)), nil
	case types.ArrayValue:
		vb := NewValueBuffer()
		err := vb.Copy(types.As[types.Array](v))
		if err != nil {
			return nil, err
		}
		return types.NewArrayValue(vb), nil
	case types.ObjectValue:
		fb := NewFieldBuffer()
		err := fb.Copy(types.As[types.Object](v))
		if err != nil {
			return nil, err
		}
		return types.NewObjectValue(fb), nil
	}

	panic(fmt.Sprintf("Unsupported value type: %s", v.Type()))
}

// Apply a function to all the values of the buffer.
func (fb *FieldBuffer) Apply(fn func(p Path, v types.Value) (types.Value, error)) error {
	path := Path{PathFragment{}}
	var err error

	for i, f := range fb.fields {
		path[0].FieldName = f.Field

		f.Value, err = fn(path, f.Value)
		if err != nil {
			return err
		}
		fb.fields[i].Value = f.Value

		switch f.Value.Type() {
		case types.ObjectValue:
			buf, ok := types.Is[*FieldBuffer](f.Value)
			if !ok {
				buf = NewFieldBuffer()
				err := buf.Copy(types.As[types.Object](f.Value))
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
			fb.fields[i].Value = types.NewObjectValue(buf)
		case types.ArrayValue:
			buf, ok := types.Is[*ValueBuffer](f.Value)
			if !ok {
				buf = NewValueBuffer()
				err := buf.Copy(types.As[types.Array](f.Value))
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
			fb.fields[i].Value = types.NewArrayValue(buf)
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

// MaskFields returns a new object that masks the given fields.
func MaskFields(d types.Object, fields ...string) types.Object {
	return &maskObject{d, fields}
}

type maskObject struct {
	d    types.Object
	mask []string
}

func (m *maskObject) Iterate(fn func(field string, value types.Value) error) error {
	return m.d.Iterate(func(field string, value types.Value) error {
		if !stringutil.Contains(m.mask, field) {
			return fn(field, value)
		}

		return nil
	})
}

func (m *maskObject) GetByField(field string) (types.Value, error) {
	if !stringutil.Contains(m.mask, field) {
		return m.d.GetByField(field)
	}

	return nil, types.ErrFieldNotFound
}

func (m *maskObject) MarshalJSON() ([]byte, error) {
	return MarshalJSON(m)
}

// OnlyFields returns a new object that only contains the given fields.
func OnlyFields(d types.Object, fields ...string) types.Object {
	return &onlyObject{d, fields}
}

type onlyObject struct {
	d      types.Object
	fields []string
}

func (o *onlyObject) Iterate(fn func(field string, value types.Value) error) error {
	for _, f := range o.fields {
		v, err := o.d.GetByField(f)
		if err != nil {
			continue
		}

		if err := fn(f, v); err != nil {
			return err
		}
	}

	return nil
}

func (o *onlyObject) GetByField(field string) (types.Value, error) {
	if stringutil.Contains(o.fields, field) {
		return o.d.GetByField(field)
	}

	return nil, types.ErrFieldNotFound
}

func (o *onlyObject) MarshalJSON() ([]byte, error) {
	return MarshalJSON(o)
}

func WithSortedFields(d types.Object) types.Object {
	return &sortedObject{d}
}

type sortedObject struct {
	types.Object
}

func (s *sortedObject) Iterate(fn func(field string, value types.Value) error) error {
	// iterate first to get the list of fields
	var fields []string
	err := s.Object.Iterate(func(field string, value types.Value) error {
		fields = append(fields, field)
		return nil
	})
	if err != nil {
		return err
	}

	// sort the fields
	sort.Strings(fields)

	// iterate again
	for _, f := range fields {
		v, err := s.Object.GetByField(f)
		if err != nil {
			continue
		}

		if err := fn(f, v); err != nil {
			return err
		}
	}

	return nil
}
