package row

import (
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

// NewValue creates a value whose type is infered from x.
func NewValue(x any) (types.Value, error) {
	if x == nil {
		return types.NewNullValue(), nil
	}
	switch v := x.(type) {
	case *int:
		if v == nil {
			return types.NewNullValue(), nil
		}
		return types.NewBigintValue(int64(*v)), nil
	case int:
		return types.NewBigintValue(int64(v)), nil
	case *int32:
		if v == nil {
			return types.NewNullValue(), nil
		}
		return types.NewBigintValue(int64(*v)), nil
	case int32:
		return types.NewBigintValue(int64(v)), nil
	case *uint64:
		if v == nil {
			return types.NewNullValue(), nil
		}
		return types.NewBigintValue(int64(*v)), nil
	case uint64:
		return types.NewBigintValue(int64(v)), nil
	case *int64:
		if v == nil {
			return types.NewNullValue(), nil
		}
		return types.NewBigintValue(*v), nil
	case int64:
		return types.NewBigintValue(v), nil
	case *float64:
		if v == nil {
			return types.NewNullValue(), nil
		}
		return types.NewDoubleValue(*v), nil
	case float64:
		return types.NewDoubleValue(v), nil
	case *bool:
		if v == nil {
			return types.NewNullValue(), nil
		}
		return types.NewBooleanValue(*v), nil
	case bool:
		return types.NewBooleanValue(v), nil
	case *[]byte:
		if v == nil {
			return types.NewNullValue(), nil
		}
		return types.NewByteaValue(*v), nil
	case []byte:
		return types.NewByteaValue(v), nil
	case *string:
		if v == nil {
			return types.NewNullValue(), nil
		}
		return types.NewTextValue(*v), nil
	case string:
		return types.NewTextValue(v), nil
	case *time.Time:
		if v == nil {
			return types.NewNullValue(), nil
		}
		return types.NewTimestampValue(*v), nil
	case time.Time:
		return types.NewTimestampValue(v), nil
	}

	return nil, errors.New("unsupported type")
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

type Column struct {
	Name  string
	Value types.Value
}

// Add a field to the buffer.
func (cb *ColumnBuffer) Add(column string, v types.Value) *ColumnBuffer {
	cb.columns = append(cb.columns, Column{column, v})
	return cb
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

func SortColumns(r Row) Row {
	return &sortedRow{r}
}

func Columns(r Row) ([]string, error) {
	var cols []string
	err := r.Iterate(func(column string, value types.Value) error {
		cols = append(cols, column)
		return nil
	})
	return cols, err
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
