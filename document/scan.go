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
			return fmt.Errorf("unsupported type %T", target)
		}

		return scanValue(v, ref)
	})
}

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

		if f.Type().Kind() == reflect.Ptr {
			err = scanValue(v, f)
		} else {
			err = scanValue(v, f)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

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

	if k == reflect.Array && ref.Len() < al {
		return errors.New("array length too small")
	}

	sref := reflect.Indirect(ref)

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
		return fmt.Errorf("unsupported type %s", ref.Type().String())
	}

	if ref.Kind() == reflect.Ptr {
		ref = reflect.Indirect(ref)
	}

	if ref.Kind() != reflect.Map {
		return fmt.Errorf("unsupported type %s", ref.Type().String())
	}

	return mapScan(d, ref)
}

func mapScan(d Document, ref reflect.Value) error {
	if ref.Type().Key().Kind() != reflect.String {
		return fmt.Errorf("unsupported type %s", ref.Type().String())
	}

	if ref.IsZero() {
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

func scanValue(v Value, ref reflect.Value) error {
	if !ref.IsValid() {
		return fmt.Errorf("unsupported type %s", ref.Type().String())
	}

	if ref.IsZero() && ref.Type().Kind() == reflect.Ptr {
		ref.Set(reflect.New(ref.Type().Elem()))
	}

	ref = reflect.Indirect(ref)

	// if the user passed a **ptr
	// make sure it points to a valid value
	// or create one
	// then dereference
	if ref.Kind() == reflect.Ptr {
		if ref.IsZero() {
			ref.Set(reflect.New(ref.Type().Elem()))
		}

		ref = reflect.Indirect(ref)
	}

	switch ref.Kind() {
	case reflect.String:
		x, err := v.ConvertToString()
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
		x, err := v.ConvertToUint64()
		if err != nil {
			return err
		}
		ref.SetUint(x)
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
			x, err := v.ConvertToBytes()
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

	return fmt.Errorf("unsupported type %s", ref.Type().String())
}
