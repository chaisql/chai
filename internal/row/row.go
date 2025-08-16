package row

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/buger/jsonparser"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
)

type Row interface {
	// Iterate goes through all the columns of the row and calls the given function
	// by passing the column name
	Iterate(fn func(column string, value types.Value) error) error

	// Get returns the value of the given column.
	// If the column does not exist, it returns ErrColumnNotFound.
	Get(name string) (types.Value, error)

	// MarshalJSON encodes the row as JSON.
	MarshalJSON() ([]byte, error)
}

// Length returns the number of columns of a row.
func Length(r Row) (int, error) {
	if cb, ok := r.(*ColumnBuffer); ok {
		return cb.Len(), nil
	}

	var len int
	err := r.Iterate(func(_ string, _ types.Value) error {
		len++
		return nil
	})
	return len, err
}

func Columns(r Row) ([]string, error) {
	var columns []string
	err := r.Iterate(func(c string, _ types.Value) error {
		columns = append(columns, c)
		return nil
	})
	return columns, err
}

// NewFromMap creates an object from a map.
// Due to the way maps are designed, iteration order is not guaranteed.
func NewFromMap[T any](m map[string]T) Row {
	return mapRow[T](m)
}

type mapRow[T any] map[string]T

var _ Row = (*mapRow[any])(nil)

func (m mapRow[T]) Iterate(fn func(column string, value types.Value) error) error {
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

func (m mapRow[T]) Get(column string) (types.Value, error) {
	v, ok := m[column]
	if !ok {
		return nil, errors.Wrapf(types.ErrColumnNotFound, "%s not found", column)
	}

	return NewValue(v)
}

// MarshalJSON implements the json.Marshaler interface.
func (m mapRow[T]) MarshalJSON() ([]byte, error) {
	return MarshalJSON(m)
}

type reflectMapObject reflect.Value

var _ Row = (*reflectMapObject)(nil)

func (m reflectMapObject) Iterate(fn func(column string, value types.Value) error) error {
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

func (m reflectMapObject) Get(column string) (types.Value, error) {
	M := reflect.Value(m)
	v := M.MapIndex(reflect.ValueOf(column))
	if v == (reflect.Value{}) {
		return nil, errors.Wrapf(types.ErrColumnNotFound, "%s not found", column)
	}
	return NewValue(v.Interface())
}

// MarshalJSON implements the json.Marshaler interface.
func (m reflectMapObject) MarshalJSON() ([]byte, error) {
	return MarshalJSON(m)
}

// NewFromStruct creates an object from a struct using reflection.
func NewFromStruct(s any) (Row, error) {
	ref := reflect.Indirect(reflect.ValueOf(s))

	if !ref.IsValid() || ref.Kind() != reflect.Struct {
		return nil, errors.New("expected struct or pointer to struct")
	}

	return newFromStruct(ref)
}

func newFromStruct(ref reflect.Value) (Row, error) {
	var cb ColumnBuffer
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
			err = d.Iterate(func(column string, value types.Value) error {
				cb.Add(column, value)
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

		column := strings.ToLower(sf.Name)
		if gtag, ok := sf.Tag.Lookup("chai"); ok {
			if gtag == "-" {
				continue
			}
			column = gtag
		}

		cb.Add(column, v)
	}

	return &cb, nil
}

// NewValue creates a value whose type is infered from x.
func NewValue(x any) (types.Value, error) {
	// Attempt exact matches first:
	switch v := x.(type) {
	case time.Duration:
		return types.NewBigintValue(v.Nanoseconds()), nil
	case time.Time:
		return types.NewTimestampValue(v), nil
	case nil:
		return types.NewNullValue(), nil
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
		return types.NewBooleanValue(v.Bool()), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return types.NewBigintValue(v.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		x := v.Uint()
		if x > math.MaxInt64 {
			return nil, fmt.Errorf("cannot convert unsigned integer struct column to int64: %d out of range", x)
		}
		return types.NewBigintValue(int64(x)), nil
	case reflect.Float32, reflect.Float64:
		return types.NewDoubleValue(v.Float()), nil
	case reflect.String:
		return types.NewTextValue(v.String()), nil
	case reflect.Slice:
		if reflect.TypeOf(v.Interface()).Elem().Kind() == reflect.Uint8 {
			return types.NewBlobValue(v.Bytes()), nil
		}
		return nil, errors.Errorf("unsupported slice type: %T", x)
	case reflect.Interface:
		if v.IsNil() {
			return types.NewNullValue(), nil
		}
		return NewValue(v.Elem().Interface())
	}

	return nil, NewErrUnsupportedType(x, "")
}

// NewFromCSV takes a list of headers and columns and returns an row.
// Each header will be assigned as the key and each corresponding column as a text value.
// The length of headers and columns must be the same.
func NewFromCSV(headers, columns []string) Row {
	fb := NewColumnBuffer()
	fb.ScanCSV(headers, columns)

	return fb
}

// ColumnBuffer stores a group of columns in memory. It implements the Row interface.
type ColumnBuffer struct {
	columns []Column
}

// NewColumnBuffer creates a ColumnBuffer.
func NewColumnBuffer() *ColumnBuffer {
	return new(ColumnBuffer)
}

// MarshalJSON implements the json.Marshaler interface.
func (cb *ColumnBuffer) MarshalJSON() ([]byte, error) {
	return MarshalJSON(cb)
}

func (cb *ColumnBuffer) UnmarshalJSON(data []byte) error {
	return jsonparser.ObjectEach(data, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		v, err := parseJSONValue(dataType, value)
		if err != nil {
			return err
		}

		cb.Add(string(key), v)
		return nil
	})
}

func (cb *ColumnBuffer) String() string {
	s, _ := cb.MarshalJSON()
	return string(s)
}

type Column struct {
	Name  string
	Value types.Value
}

// Add a field to the buffer.
func (cb *ColumnBuffer) Add(column string, v types.Value) *ColumnBuffer {
	cb.columns = append(cb.columns, Column{column, v})
	return cb
}

// ScanRow copies all the columns of d to the buffer.
func (cb *ColumnBuffer) ScanRow(r Row) error {
	return r.Iterate(func(f string, v types.Value) error {
		cb.Add(f, v)
		return nil
	})
}

// Get returns a value by column. Returns an error if the column doesn't exists.
func (cb ColumnBuffer) Get(column string) (types.Value, error) {
	for _, fv := range cb.columns {
		if fv.Name == column {
			return fv.Value, nil
		}
	}

	return nil, errors.Wrapf(types.ErrColumnNotFound, "%s not found", column)
}

// Set replaces a column if it already exists or creates one if not.
func (cb *ColumnBuffer) Set(column string, v types.Value) error {
	_, err := cb.Get(column)
	if errors.Is(err, types.ErrColumnNotFound) {
		cb.Add(column, v)
		return nil
	}
	if err != nil {
		return err
	}

	_ = cb.Replace(column, v)
	return nil
}

// Iterate goes through all the columns of the row and calls the given function by passing each one of them.
// If the given function returns an error, the iteration stops.
func (cb ColumnBuffer) Iterate(fn func(column string, value types.Value) error) error {
	for _, cv := range cb.columns {
		err := fn(cv.Name, cv.Value)
		if err != nil {
			return err
		}
	}

	return nil
}

// Delete a column from the buffer.
func (cb *ColumnBuffer) Delete(column string) error {
	for i := range cb.columns {
		if cb.columns[i].Name == column {
			cb.columns = append(cb.columns[0:i], cb.columns[i+1:]...)
			return nil
		}
	}

	return errors.Wrapf(types.ErrColumnNotFound, "%s not found", column)
}

// Replace the value of the column by v.
func (cb *ColumnBuffer) Replace(column string, v types.Value) error {
	for i := range cb.columns {
		if cb.columns[i].Name == column {
			cb.columns[i].Value = v
			return nil
		}
	}

	return errors.Wrapf(types.ErrColumnNotFound, "%s not found", column)
}

// Copy every value of the row to the buffer.
func (cb *ColumnBuffer) Copy(r Row) error {
	return r.Iterate(func(column string, value types.Value) error {
		cb.Add(strings.Clone(column), value)
		return nil
	})
}

// Apply a function to all the values of the buffer.
func (cb *ColumnBuffer) Apply(fn func(column string, v types.Value) (types.Value, error)) error {
	var err error

	for i, c := range cb.columns {
		cb.columns[i].Value, err = fn(c.Name, c.Value)
		if err != nil {
			return err
		}
	}

	return nil
}

// Len of the buffer.
func (cb ColumnBuffer) Len() int {
	return len(cb.columns)
}

// Reset the buffer.
func (cb *ColumnBuffer) Reset() {
	cb.columns = cb.columns[:0]
}

func (cb *ColumnBuffer) ScanCSV(headers, columns []string) {
	for i, h := range headers {
		if i >= len(columns) {
			break
		}

		cb.Add(h, types.NewTextValue(columns[i]))
	}
}

func SortColumns(r Row) Row {
	return &sortedRow{r}
}

type sortedRow struct {
	Row
}

func (s *sortedRow) Iterate(fn func(column string, value types.Value) error) error {
	// iterate first to get the list of columns
	var columns []string
	err := s.Row.Iterate(func(column string, value types.Value) error {
		columns = append(columns, column)
		return nil
	})
	if err != nil {
		return err
	}

	// sort the fields
	sort.Strings(columns)

	// iterate again
	for _, f := range columns {
		v, err := s.Row.Get(f)
		if err != nil {
			continue
		}

		if err := fn(f, v); err != nil {
			return err
		}
	}

	return nil
}

func Flatten(r Row) []types.Value {
	var values []types.Value
	_ = r.Iterate(func(column string, v types.Value) error {
		values = append(values, types.NewTextValue(column))
		values = append(values, v)
		return nil
	})
	return values
}

func Unflatten(values []types.Value) Row {
	cb := NewColumnBuffer()
	for i := 0; i < len(values); i += 2 {
		cb.Add(types.AsString(values[i]), values[i+1])
	}
	return cb
}
