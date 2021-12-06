// Package document defines types to manipulate and compare documents and values.
package document

import (
	"strconv"
	"strings"

	"github.com/buger/jsonparser"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/types"
)

// ErrFieldNotFound must be returned by Document implementations, when calling the GetByField method and
// the field wasn't found in the document.
var ErrFieldNotFound = errors.New("field not found")

// ErrUnsupportedType is used to skip struct or array fields that are not supported.
type ErrUnsupportedType struct {
	Value interface{}
	Msg   string
}

func (e *ErrUnsupportedType) Error() string {
	return stringutil.Sprintf("unsupported type %T. %s", e.Value, e.Msg)
}

// An Iterator can iterate over documents.
type Iterator interface {
	// Iterate goes through all the documents and calls the given function by passing each one of them.
	// If the given function returns an error, the iteration stops.
	Iterate(fn func(d types.Document) error) error
}

// MarshalJSON encodes a document to json.
func MarshalJSON(d types.Document) ([]byte, error) {
	return types.NewDocumentValue(d).MarshalJSON()
}

// MarshalJSONArray encodes an array to json.
func MarshalJSONArray(a types.Array) ([]byte, error) {
	return types.NewArrayValue(a).MarshalJSON()
}

// Length returns the length of a document.
func Length(d types.Document) (int, error) {
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

// FieldBuffer stores a group of fields in memory. It implements the Document interface.
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

// ScanDocument copies all the fields of d to the buffer.
func (fb *FieldBuffer) ScanDocument(d types.Document) error {
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

	return nil, ErrFieldNotFound
}

// setFieldValue replaces a field if it already exists or creates one if not.
func (fb *FieldBuffer) setFieldValue(field string, reqValue types.Value) error {
	_, err := fb.GetByField(field)
	switch err {
	case ErrFieldNotFound:
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
	case types.DocumentValue:
		var buf FieldBuffer
		err := buf.ScanDocument(v.V().(types.Document))
		if err != nil {
			return v, err
		}

		if len(p) == 1 {
			err = buf.setFieldValue(p[0].FieldName, newValue)
			return types.NewDocumentValue(&buf), err
		}

		// the field is a document but the path expects an array,
		// return an error
		if p[0].FieldName == "" {
			return nil, ErrFieldNotFound
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
		return types.NewDocumentValue(&buf), err
	case types.ArrayValue:
		var vb ValueBuffer
		err := vb.ScanArray(v.V().(types.Array))
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

	return nil, ErrFieldNotFound
}

// Set replaces a field if it already exists or creates one if not.
// TODO(asdine): Set should always fail with ErrFieldNotFound if the path
// doesn't resolve to an existing field.
func (fb *FieldBuffer) Set(path Path, v types.Value) error {
	if len(path) == 0 || path[0].FieldName == "" {
		return ErrFieldNotFound
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

	fb.setFieldValue(path[0].FieldName, va)

	return nil
}

// Iterate goes through all the fields of the document and calls the given function by passing each one of them.
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
	v, err := parentPath.GetValueFromDocument(fb)
	if err != nil {
		return err
	}
	switch v.Type() {
	case types.DocumentValue:
		subBuf, ok := v.V().(*FieldBuffer)
		if !ok {
			return errors.New("Delete doesn't support non buffered document")
		}

		for i := range subBuf.fields {
			if subBuf.fields[i].Field == lastFragment.FieldName {
				subBuf.fields = append(subBuf.fields[0:i], subBuf.fields[i+1:]...)
				return nil
			}
		}

		return ErrFieldNotFound
	case types.ArrayValue:
		subBuf, ok := v.V().(*ValueBuffer)
		if !ok {
			return errors.New("Delete doesn't support non buffered array")
		}

		idx := path[len(path)-1].ArrayIndex
		if idx >= len(subBuf.Values) {
			return ErrFieldNotFound
		}
		subBuf.Values = append(subBuf.Values[0:idx], subBuf.Values[idx+1:]...)
	default:
		return ErrFieldNotFound
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

	return ErrFieldNotFound
}

// Copy deep copies every value of the document to the buffer.
// If a value is a document or an array, it will be stored as a FieldBuffer or ValueBuffer respectively.
func (fb *FieldBuffer) Copy(d types.Document) error {
	err := fb.ScanDocument(d)
	if err != nil {
		return err
	}

	for i, f := range fb.fields {
		switch f.Value.Type() {
		case types.DocumentValue:
			var buf FieldBuffer
			err = buf.Copy(f.Value.V().(types.Document))
			if err != nil {
				return err
			}

			fb.fields[i].Value = types.NewDocumentValue(&buf)
		case types.ArrayValue:
			var buf ValueBuffer
			err = buf.Copy(f.Value.V().(types.Array))
			if err != nil {
				return err
			}

			fb.fields[i].Value = types.NewArrayValue(&buf)
		}
	}

	return nil
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
		case types.DocumentValue:
			buf, ok := f.Value.V().(*FieldBuffer)
			if !ok {
				buf = NewFieldBuffer()
				err := buf.Copy(f.Value.V().(types.Document))
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
			fb.fields[i].Value = types.NewDocumentValue(buf)
		case types.ArrayValue:
			buf, ok := f.Value.V().(*ValueBuffer)
			if !ok {
				buf = NewValueBuffer()
				err := buf.Copy(f.Value.V().(types.Array))
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

// A Path represents the path to a particular value within a document.
type Path []PathFragment

// NewPath creates a path from a list of strings representing either a field name
// or an array index in string form.
func NewPath(fragments ...string) Path {
	var path Path

	for _, frag := range fragments {
		idx, err := strconv.Atoi(frag)
		if err != nil {
			path = append(path, PathFragment{FieldName: frag})
		} else {
			path = append(path, PathFragment{ArrayIndex: idx})
		}
	}

	return path
}

// PathFragment is a fragment of a path representing either a field name or
// the index of an array.
type PathFragment struct {
	FieldName  string
	ArrayIndex int
}

// String representation of all the fragments of the path.
// It implements the Stringer interface.
func (p Path) String() string {
	var b strings.Builder

	for i := range p {
		if p[i].FieldName != "" {
			if i != 0 {
				b.WriteRune('.')
			}
			b.WriteString(p[i].FieldName)
		} else {
			b.WriteString("[" + strconv.Itoa(p[i].ArrayIndex) + "]")
		}
	}
	return b.String()
}

// IsEqual returns whether other is equal to p.
func (p Path) IsEqual(other Path) bool {
	if len(other) != len(p) {
		return false
	}

	for i := range p {
		if other[i] != p[i] {
			return false
		}
	}

	return true
}

// GetValueFromDocument returns the value at path p from d.
func (p Path) GetValueFromDocument(d types.Document) (types.Value, error) {
	if len(p) == 0 {
		return nil, errors.Wrap(ErrFieldNotFound)
	}
	if p[0].FieldName == "" {
		return nil, errors.Wrap(ErrFieldNotFound)
	}

	v, err := d.GetByField(p[0].FieldName)
	if err != nil {
		return nil, err
	}

	if len(p) == 1 {
		return v, nil
	}

	return p[1:].getValueFromValue(v)
}

// GetValueFromArray returns the value at path p from a.
func (p Path) GetValueFromArray(a types.Array) (types.Value, error) {
	if len(p) == 0 {
		return nil, errors.Wrap(ErrFieldNotFound)
	}
	if p[0].FieldName != "" {
		return nil, errors.Wrap(ErrFieldNotFound)
	}

	v, err := a.GetByIndex(p[0].ArrayIndex)
	if err != nil {
		if errors.Is(err, ErrValueNotFound) {
			return nil, errors.Wrap(ErrFieldNotFound)
		}

		return nil, err
	}

	if len(p) == 1 {
		return v, nil
	}

	return p[1:].getValueFromValue(v)
}

func (p Path) Clone() Path {
	c := make(Path, len(p))
	copy(c, p)
	return c
}

func (p Path) getValueFromValue(v types.Value) (types.Value, error) {
	switch v.Type() {
	case types.DocumentValue:
		return p.GetValueFromDocument(v.V().(types.Document))
	case types.ArrayValue:
		return p.GetValueFromArray(v.V().(types.Array))
	}

	return nil, ErrFieldNotFound
}

type Paths []Path

func (p Paths) String() string {
	var sb strings.Builder

	for i, pt := range p {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(pt.String())
	}

	return sb.String()
}

// IsEqual returns whether other is equal to p.
func (p Paths) IsEqual(other Paths) bool {
	if len(other) != len(p) {
		return false
	}

	for i := range p {
		if !other[i].IsEqual(p[i]) {
			return false
		}
	}

	return true
}
