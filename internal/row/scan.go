package row

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

// ErrUnsupportedType is used to skip struct or array fields that are not supported.
type ErrUnsupportedType struct {
	Value interface{}
	Msg   string
}

func NewErrUnsupportedType(value any, msg string) error {
	return errors.WithStack(&ErrUnsupportedType{
		Value: value,
		Msg:   msg,
	})
}

func (e *ErrUnsupportedType) Error() string {
	return fmt.Sprintf("unsupported type %T. %s", e.Value, e.Msg)
}

// A RowScanner can iterate over a row and scan all the columns.
type RowScanner interface {
	ScanRow(Row) error
}

// Scan each field of the object into the given variables.
func Scan(r Row, targets ...any) error {
	var i int

	return r.Iterate(func(c string, v types.Value) error {
		if i >= len(targets) {
			return errors.New("target list too small")
		}

		target := targets[i]
		i++

		ref := reflect.ValueOf(target)
		if !ref.IsValid() {
			return NewErrUnsupportedType(target, fmt.Sprintf("Parameter %d is not valid", i))
		}

		return scanValue(v, ref)
	})
}

// StructScan scans d into t. t is expected to be a pointer to a struct.
//
// By default, each struct field name is lowercased and the row's Get method
// is called with that name. If there is a match, the value is converted to the struct
// field type when possible, otherwise an error is returned.
// The decoding of each struct field can be customized by the format string stored
// under the "chai" key stored in the struct field's tag.
// The content of the format string is used instead of the struct field name and passed
// to the Get method.
func StructScan(r Row, t any) error {
	if cb, ok := t.(*ColumnBuffer); ok {
		return cb.Copy(r)
	}

	ref := reflect.ValueOf(t)

	if !ref.IsValid() || ref.Kind() != reflect.Ptr {
		return errors.New("target must be pointer to a valid Go type")
	}

	if ref.Elem().Kind() != reflect.Struct {
		return errors.New("target must be pointer to a struct")
	}

	if ref.IsNil() {
		ref.Set(reflect.New(ref.Type().Elem()))
	}

	return structScan(r, ref)
}

func structScan(r Row, ref reflect.Value) error {
	if ref.Type().Implements(reflect.TypeOf((*RowScanner)(nil)).Elem()) {
		return ref.Interface().(RowScanner).ScanRow(r)
	}

	sref := reflect.Indirect(ref)
	stp := sref.Type()
	l := sref.NumField()
	for i := 0; i < l; i++ {
		f := sref.Field(i)
		sf := stp.Field(i)
		if sf.Anonymous {
			err := structScan(r, f)
			if err != nil {
				return err
			}
			continue
		}
		var name string
		if gtag, ok := sf.Tag.Lookup("chai"); ok {
			if gtag == "-" {
				continue
			}

			name = gtag
		} else {
			name = strings.ToLower(sf.Name)
		}
		v, err := r.Get(name)
		if errors.Is(err, types.ErrColumnNotFound) {
			v = types.NewNullValue()
		} else if err != nil {
			return err
		}

		if err := scanValue(v, f); err != nil {
			return err
		}
	}

	return nil
}

// MapScan decodes the row into a map.
func MapScan(r Row, t any) error {
	ref := reflect.ValueOf(t)
	if !ref.IsValid() {
		return NewErrUnsupportedType(ref, "t must be a valid reference")
	}

	if ref.Kind() == reflect.Ptr {
		ref = reflect.Indirect(ref)
	}

	if ref.Kind() != reflect.Map {
		return NewErrUnsupportedType(ref, "t is not a map")
	}

	return mapScan(r, ref)
}

func mapScan(r Row, ref reflect.Value) error {
	if ref.Type().Key().Kind() != reflect.String {
		return NewErrUnsupportedType(ref, "map key must be a string")
	}

	if ref.IsNil() {
		ref.Set(reflect.MakeMap(ref.Type()))
	}

	return r.Iterate(func(f string, v types.Value) error {
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
func ScanValue(v types.Value, t any) error {
	return scanValue(v, reflect.ValueOf(t))
}

func scanValue(v types.Value, ref reflect.Value) error {
	if !ref.IsValid() {
		return NewErrUnsupportedType(ref, "parameter is not a valid reference")
	}

	if v.Type() == types.TypeNull {
		if ref.Type().Kind() != reflect.Ptr {
			return nil
		}

		if ref.IsNil() {
			return nil
		}

		if !ref.CanSet() {
			ref = reflect.Indirect(ref)
		}

		ref.Set(reflect.Zero(ref.Type()))
		return nil
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

	// Scan nulls as Go zero values.
	if v.Type() == types.TypeNull {
		ref.Set(reflect.Zero(ref.Type()))
		return nil
	}

	switch ref.Kind() {
	case reflect.String:
		v, err := v.CastAs(types.TypeText)
		if err != nil {
			return err
		}
		ref.SetString(types.AsString(v))
		return nil
	case reflect.Bool:
		v, err := v.CastAs(types.TypeBoolean)
		if err != nil {
			return err
		}
		ref.SetBool(types.AsBool(v))
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v, err := v.CastAs(types.TypeBigint)
		if err != nil {
			return err
		}
		x := types.AsInt64(v)
		if x < 0 {
			return fmt.Errorf("cannot convert value %d into Go value of type %s", x, ref.Type().Name())
		}
		ref.SetUint(uint64(x))
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v, err := v.CastAs(types.TypeBigint)
		if err != nil {
			return err
		}
		ref.SetInt(types.AsInt64(v))
		return nil
	case reflect.Float32, reflect.Float64:
		v, err := v.CastAs(types.TypeDouble)
		if err != nil {
			return err
		}
		ref.SetFloat(types.AsFloat64(v))
		return nil
	case reflect.Interface:
		if !ref.IsNil() {
			return scanValue(v, ref.Elem())
		}
		switch v.Type() {
		case types.TypeText:
			// copy the string to avoid
			// keeping a reference to the underlying buffer
			// which could be reused
			cp := strings.Clone(types.AsString(v))
			ref.Set(reflect.ValueOf(cp))
			return nil
		case types.TypeBlob:
			// copy the byte slice to avoid
			// keeping a reference to the underlying buffer
			// which could be reused
			b := bytes.Clone(types.AsByteSlice(v))
			ref.Set(reflect.ValueOf(b))
			return nil
		}

		ref.Set(reflect.ValueOf(v.V()))
		return nil
	case reflect.Slice:
		if ref.Type().Elem().Kind() == reflect.Uint8 {
			if v.Type() != types.TypeText && v.Type() != types.TypeBlob {
				return fmt.Errorf("cannot scan value of type %s to byte slice", v.Type())
			}
			if v.Type() == types.TypeText {
				ref.SetBytes([]byte(types.AsString(v)))
			} else {
				ref.SetBytes(types.AsByteSlice(v))
			}
			return nil
		}
		return NewErrUnsupportedType(ref.Interface(), "Invalid type")
	case reflect.Array:
		if ref.Type().Elem().Kind() == reflect.Uint8 {
			if v.Type() != types.TypeText && v.Type() != types.TypeBlob {
				return fmt.Errorf("cannot scan value of type %s to byte slice", v.Type())
			}
			reflect.Copy(ref, reflect.ValueOf(v.V()))
			return nil
		}
		return NewErrUnsupportedType(ref.Interface(), "Invalid type")
	}

	// test with supported stdlib types
	switch ref.Type().String() {
	case "time.Time":
		switch v.Type() {
		case types.TypeText:
			parsed, err := time.Parse(time.RFC3339Nano, types.AsString(v))
			if err != nil {
				return err
			}

			ref.Set(reflect.ValueOf(parsed))
			return nil
		case types.TypeTimestamp:
			ref.Set(reflect.ValueOf(types.AsTime(v)))
			return nil
		}
	}

	return NewErrUnsupportedType(ref.Interface(), "Invalid type")
}

// ScanRow scans a row into dest which must be either a struct pointer, a map or a map pointer.
func ScanRow(r Row, t any) error {
	ref := reflect.ValueOf(t)

	if !ref.IsValid() {
		return errors.New("target must be pointer to a valid Go type")
	}

	switch reflect.Indirect(ref).Kind() {
	case reflect.Map:
		return mapScan(r, ref)
	case reflect.Struct:
		if ref.IsNil() {
			ref.Set(reflect.New(ref.Type().Elem()))
		}
		return structScan(r, ref)
	default:
		return errors.New("target must be a either a pointer to struct, a map or a map pointer")
	}
}

// ScanColumn scans a single column into dest.
func ScanColumn(r Row, column string, dest any) error {
	v, err := r.Get(column)
	if err != nil {
		return err
	}

	return ScanValue(v, dest)
}
