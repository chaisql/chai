package document

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/buger/jsonparser"
	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/types"
)

// NewFromJSON creates a document from raw JSON data.
// The returned document will lazily decode the data.
// If data is not a valid json object, calls to Iterate or GetByField will
// return an error.
func NewFromJSON(data []byte) types.Document {
	return &jsonEncodedDocument{data}
}

type jsonEncodedDocument struct {
	data []byte
}

func (j jsonEncodedDocument) Iterate(fn func(field string, value types.Value) error) error {
	return jsonparser.ObjectEach(j.data, func(key, value []byte, dataType jsonparser.ValueType, offset int) error {
		v, err := parseJSONValue(dataType, value)
		if err != nil {
			return err
		}

		return fn(string(key), v)
	})
}

func (j jsonEncodedDocument) GetByField(field string) (types.Value, error) {
	v, dt, _, err := jsonparser.Get(j.data, field)
	if dt == jsonparser.NotExist {
		return nil, types.ErrFieldNotFound
	}
	if err != nil {
		return nil, err
	}

	return parseJSONValue(dt, v)
}

func (j jsonEncodedDocument) MarshalJSON() ([]byte, error) {
	return j.data, nil
}

// NewFromMap creates a document from a map.
// Due to the way maps are designed, iteration order is not guaranteed.
func NewFromMap[T any](m map[string]T) types.Document {
	return mapDocument[T](m)
}

type mapDocument[T any] map[string]T

var _ types.Document = (*mapDocument[any])(nil)

func (m mapDocument[T]) Iterate(fn func(field string, value types.Value) error) error {
	for k, v := range m {
		v, err := NewValue(v)
		if err != nil {
			return err
		}

		err = fn(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m mapDocument[T]) GetByField(field string) (types.Value, error) {
	v, ok := m[field]
	if !ok {
		return nil, types.ErrFieldNotFound
	}

	return NewValue(v)
}

// MarshalJSON implements the json.Marshaler interface.
func (m mapDocument[T]) MarshalJSON() ([]byte, error) {
	return MarshalJSON(m)
}

type reflectMapDocument reflect.Value

var _ types.Document = (*reflectMapDocument)(nil)

func (m reflectMapDocument) Iterate(fn func(field string, value types.Value) error) error {
	M := reflect.Value(m)
	it := M.MapRange()

	for it.Next() {
		v, err := NewValue(it.Value().Interface())
		if err != nil {
			return err
		}

		err = fn(it.Key().String(), v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m reflectMapDocument) GetByField(field string) (types.Value, error) {
	M := reflect.Value(m)
	v := M.MapIndex(reflect.ValueOf(field))
	if v == (reflect.Value{}) {
		return nil, types.ErrFieldNotFound
	}
	return NewValue(v.Interface())
}

// MarshalJSON implements the json.Marshaler interface.
func (m reflectMapDocument) MarshalJSON() ([]byte, error) {
	return MarshalJSON(m)
}

// NewFromStruct creates a document from a struct using reflection.
func NewFromStruct(s interface{}) (types.Document, error) {
	ref := reflect.Indirect(reflect.ValueOf(s))

	if !ref.IsValid() || ref.Kind() != reflect.Struct {
		return nil, errors.New("expected struct or pointer to struct")
	}

	return newFromStruct(ref)
}

func newFromStruct(ref reflect.Value) (types.Document, error) {
	var fb FieldBuffer
	l := ref.NumField()
	tp := ref.Type()

	for i := 0; i < l; i++ {
		f := ref.Field(i)
		if !f.IsValid() {
			continue
		}

		if f.Kind() == reflect.Ptr {
			if f.IsNil() {
				continue
			}

			f = f.Elem()
		}

		sf := tp.Field(i)

		isUnexported := sf.PkgPath != ""

		if sf.Anonymous {
			if isUnexported && f.Kind() != reflect.Struct {
				continue
			}
			d, err := newFromStruct(f)
			if err != nil {
				return nil, err
			}
			err = d.Iterate(func(field string, value types.Value) error {
				fb.Add(field, value)
				return nil
			})
			if err != nil {
				return nil, err
			}
			continue
		} else if isUnexported {
			continue
		}

		v, err := NewValue(f.Interface())
		if err != nil {
			return nil, err
		}

		field := strings.ToLower(sf.Name)
		if gtag, ok := sf.Tag.Lookup("genji"); ok {
			if gtag == "-" {
				continue
			}
			field = gtag
		}

		fb.Add(field, v)
	}

	return &fb, nil
}

// NewValue creates a value whose type is infered from x.
func NewValue(x interface{}) (types.Value, error) {
	// Attempt exact matches first:
	switch v := x.(type) {
	case time.Duration:
		return types.NewIntegerValue(v.Nanoseconds()), nil
	case time.Time:
		return types.NewTextValue(v.Format(time.RFC3339Nano)), nil
	case nil:
		return types.NewNullValue(), nil
	case types.Document:
		return types.NewDocumentValue(v), nil
	case types.Array:
		return types.NewArrayValue(v), nil
	}

	// Compare by kind to detect type definitions over built-in types.
	v := reflect.ValueOf(x)
	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			return types.NewNullValue(), nil
		}
		return NewValue(reflect.Indirect(v).Interface())
	case reflect.Bool:
		return types.NewBoolValue(v.Bool()), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return types.NewIntegerValue(v.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		x := v.Uint()
		if x > math.MaxInt64 {
			return nil, fmt.Errorf("cannot convert unsigned integer struct field to int64: %d out of range", x)
		}
		return types.NewIntegerValue(int64(x)), nil
	case reflect.Float32, reflect.Float64:
		return types.NewDoubleValue(v.Float()), nil
	case reflect.String:
		return types.NewTextValue(v.String()), nil
	case reflect.Interface:
		if v.IsNil() {
			return types.NewNullValue(), nil
		}
		return NewValue(v.Elem().Interface())
	case reflect.Struct:
		doc, err := NewFromStruct(x)
		if err != nil {
			return nil, err
		}
		return types.NewDocumentValue(doc), nil
	case reflect.Array:
		return types.NewArrayValue(&sliceArray{v}), nil
	case reflect.Slice:
		if reflect.TypeOf(v.Interface()).Elem().Kind() == reflect.Uint8 {
			return types.NewBlobValue(v.Bytes()), nil
		}
		if v.IsNil() {
			return types.NewNullValue(), nil
		}
		return types.NewArrayValue(&sliceArray{ref: v}), nil
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			return nil, &ErrUnsupportedType{x, "parameter must be a map with a string key"}
		}

		// use fast generic map if possible
		switch v.Type().Elem().Kind() {
		case reflect.Bool:
			return types.NewDocumentValue(NewFromMap(x.(map[string]bool))), nil
		case reflect.Int:
			return types.NewDocumentValue(NewFromMap(x.(map[string]int))), nil
		case reflect.Int8:
			return types.NewDocumentValue(NewFromMap(x.(map[string]int8))), nil
		case reflect.Int16:
			return types.NewDocumentValue(NewFromMap(x.(map[string]int16))), nil
		case reflect.Int32:
			return types.NewDocumentValue(NewFromMap(x.(map[string]int32))), nil
		case reflect.Int64:
			return types.NewDocumentValue(NewFromMap(x.(map[string]int64))), nil
		case reflect.Float32:
			return types.NewDocumentValue(NewFromMap(x.(map[string]float32))), nil
		case reflect.Float64:
			return types.NewDocumentValue(NewFromMap(x.(map[string]float64))), nil
		case reflect.String:
			return types.NewDocumentValue(NewFromMap(x.(map[string]string))), nil
		case reflect.Interface:
			return types.NewDocumentValue(NewFromMap(x.(map[string]any))), nil
		}

		// use reflect based map for other types
		return types.NewDocumentValue(reflectMapDocument(v)), nil
	}

	return nil, &ErrUnsupportedType{x, ""}
}

type sliceArray struct {
	ref reflect.Value
}

var _ types.Array = (*sliceArray)(nil)

func (s sliceArray) Iterate(fn func(i int, v types.Value) error) error {
	l := s.ref.Len()

	for i := 0; i < l; i++ {
		f := s.ref.Index(i)

		v, err := NewValue(f.Interface())
		if err != nil {
			if err.(*ErrUnsupportedType) != nil {
				continue
			}
			return err
		}

		err = fn(i, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s sliceArray) GetByIndex(i int) (types.Value, error) {
	if i >= s.ref.Len() {
		return nil, types.ErrFieldNotFound
	}

	v := s.ref.Index(i)
	if !v.IsValid() {
		return nil, types.ErrFieldNotFound
	}

	return NewValue(v.Interface())
}

func (s sliceArray) MarshalJSON() ([]byte, error) {
	return MarshalJSONArray(s)
}

// NewFromCSV takes a list of headers and columns and returns a document.
// Each header will be assigned as the key and each corresponding column as a text value.
// The length of headers and columns must be the same.
func NewFromCSV(headers, columns []string) types.Document {
	fb := NewFieldBuffer()
	for i, h := range headers {
		if i >= len(columns) {
			break
		}

		fb.Add(h, types.NewTextValue(columns[i]))
	}

	return fb
}
