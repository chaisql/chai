// +build !wasm

package document

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// A Scanner can iterate over a document and scan all the fields.
type Scanner interface {
	ScanDocument(Document) error
}

// Scan each field of the document into the given variables.
func Scan(d Document, targets ...interface{}) error {
	var i int

	return d.Iterate(func(f string, v Value) error {
		if i >= len(targets) {
			return errors.New("target list too small")
		}

		target := targets[i]
		i++

		ref := reflect.ValueOf(target)
		if !ref.IsValid() {
			return &ErrUnsupportedType{target, fmt.Sprintf("Parameter %d is not valid", i)}
		}

		return scanValue(v, ref)
	})
}

// StructScan scans d into t. t is expected to be a pointer to a struct.
//
// By default, each struct field name is lowercased and the document's GetByField method
// is called with that name. If there is a match, the value is converted to the struct
// field type when possible, otherwise an error is returned.
// The decoding of each struct field can be customized by the format string stored
// under the "genji" key stored in the struct field's tag.
// The content of the format string is used instead of the struct field name and passed
// to the GetByField method.
func StructScan(d Document, t interface{}) error {
	ref := reflect.ValueOf(t)

	if !ref.IsValid() || ref.Kind() != reflect.Ptr {
		return errors.New("target must be pointer to a valid Go type")
	}

	if ref.IsNil() {
		ref.Set(reflect.New(ref.Type().Elem()))
	}

	return structScan(d, ref)
}

func structScan(d Document, ref reflect.Value) error {
	if ref.Type().Implements(reflect.TypeOf((*Scanner)(nil)).Elem()) {
		return ref.Interface().(Scanner).ScanDocument(d)
	}

	sref := reflect.Indirect(ref)
	stp := sref.Type()
	l := sref.NumField()
	for i := 0; i < l; i++ {
		f := sref.Field(i)
		sf := stp.Field(i)
		var name string
		if gtag, ok := sf.Tag.Lookup("genji"); ok {
			if gtag == "-" {
				continue
			}

			name = gtag
		} else {
			name = strings.ToLower(sf.Name)
		}
		v, err := d.GetByField(name)
		if err == ErrFieldNotFound {
			continue
		}
		if err != nil {
			return err
		}

		if err := scanValue(v, f); err != nil {
			return err
		}
	}

	return nil
}

// SliceScan scans a document array into a slice or fixed size array. t must be a pointer
// to a valid slice or array.
//
// It t is a slice pointer and its capacity is too low, a new slice will be allocated.
// Otherwise, its length is set to 0 so that its content is overrided.
//
// If t is an array pointer, its capacity must be bigger than the length of a, otherwise an error is
// returned.
func SliceScan(a Array, t interface{}) error {
	return sliceScan(a, reflect.ValueOf(t))
}

func sliceScan(a Array, ref reflect.Value) error {
	if !ref.IsValid() || ref.Kind() != reflect.Ptr || ref.IsNil() {
		return errors.New("target must be pointer to a slice or array")
	}

	tp := ref.Type()
	k := tp.Elem().Kind()
	if k != reflect.Array && k != reflect.Slice {
		return errors.New("target must be pointer to a slice or array")
	}

	al, err := ArrayLength(a)
	if err != nil {
		return err
	}

	sref := reflect.Indirect(ref)

	// if array, make sure it is big enough
	if k == reflect.Array && sref.Len() < al {
		return errors.New("array length too small")
	}

	// if slice, reduce its length to 0 to overwrite the buffer
	if k == reflect.Slice {
		if sref.Cap() < al {
			sref.Set(reflect.MakeSlice(tp.Elem(), 0, al))
		} else {
			sref.SetLen(0)
		}
	}

	stp := sref.Type()

	err = a.Iterate(func(i int, v Value) error {
		if k == reflect.Array {
			err := scanValue(v, sref.Index(i).Addr())
			if err != nil {
				return err
			}
		} else {
			newV := reflect.New(stp.Elem())

			err := scanValue(v, newV)
			if err != nil {
				return err
			}

			if stp.Elem().Kind() == reflect.Ptr {
				sref = reflect.Append(sref, newV)
			} else {
				sref = reflect.Append(sref, reflect.Indirect(newV))
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	if k == reflect.Slice {
		ref.Elem().Set(sref)
	}

	return nil
}

// MapScan decodes the document into a map.
func MapScan(d Document, t interface{}) error {
	ref := reflect.ValueOf(t)
	if !ref.IsValid() {
		return &ErrUnsupportedType{ref, "t must be a valid reference"}
	}

	if ref.Kind() == reflect.Ptr {
		ref = reflect.Indirect(ref)
	}

	if ref.Kind() != reflect.Map {
		return &ErrUnsupportedType{ref, "t is not a map"}
	}

	return mapScan(d, ref)
}

func mapScan(d Document, ref reflect.Value) error {
	if ref.Type().Key().Kind() != reflect.String {
		return &ErrUnsupportedType{ref, "map key must be a string"}
	}

	if ref.IsNil() {
		ref.Set(reflect.MakeMap(ref.Type()))
	}

	return d.Iterate(func(f string, v Value) error {
		newV := reflect.New(ref.Type().Elem())

		err := scanValue(v, newV)
		if err != nil {
			return err
		}

		ref.SetMapIndex(reflect.ValueOf(f), newV.Elem())
		return nil
	})
}

// ScanValue scans v into t.
func ScanValue(v Value, t interface{}) error {
	return scanValue(v, reflect.ValueOf(t))
}

func scanValue(v Value, ref reflect.Value) error {
	if !ref.IsValid() {
		return &ErrUnsupportedType{ref, "parameter is not a valid reference"}
	}

	if ref.Type().Kind() == reflect.Ptr && ref.IsNil() {
		ref.Set(reflect.New(ref.Type().Elem()))
	}

	ref = reflect.Indirect(ref)

	// if the user passed a **ptr
	// make sure it points to a valid value
	// or create one
	// then dereference
	if ref.Kind() == reflect.Ptr {
		if ref.IsNil() {
			ref.Set(reflect.New(ref.Type().Elem()))
		}

		ref = reflect.Indirect(ref)
	}

	switch ref.Kind() {
	case reflect.String:
		x, err := v.ConvertToText()
		if err != nil {
			return err
		}
		ref.SetString(x)
		return nil
	case reflect.Bool:
		x, err := v.ConvertToBool()
		if err != nil {
			return err
		}
		ref.SetBool(x)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		x, err := v.ConvertToInt64()
		if err != nil {
			return err
		}
		if x < 0 {
			return fmt.Errorf("cannot convert value %d into Go value of type %s", x, ref.Type().Name())
		}
		ref.SetUint(uint64(x))
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		x, err := v.ConvertToInt64()
		if err != nil {
			return err
		}
		ref.SetInt(x)
		return nil
	case reflect.Float32, reflect.Float64:
		x, err := v.ConvertToFloat64()
		if err != nil {
			return err
		}
		ref.SetFloat(x)
		return nil
	case reflect.Struct:
		d, err := v.ConvertToDocument()
		if err != nil {
			return err
		}

		return structScan(d, ref)
	case reflect.Slice:
		if ref.Type().Elem().Kind() == reflect.Uint8 {
			x, err := v.ConvertToBlob()
			if err != nil {
				return err
			}
			ref.SetBytes(x)
			return nil
		}
		a, err := v.ConvertToArray()
		if err != nil {
			return err
		}

		return sliceScan(a, ref.Addr())
	case reflect.Map:
		d, err := v.ConvertToDocument()
		if err != nil {
			return err
		}

		return mapScan(d, ref)
	case reflect.Interface:
		ref.Set(reflect.ValueOf(v.V))
		return nil
	}

	return &ErrUnsupportedType{ref, "Invalid type"}
}

// Scan v into t.
func (v Value) Scan(t interface{}) error {
	return scanValue(v, reflect.ValueOf(t))
}
