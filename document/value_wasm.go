package document

import (
	"time"
)

// NewValue creates a value from x. It only supports a few type and doesn't rely on reflection.
func NewValue(x interface{}) (Value, error) {
	switch v := x.(type) {
	case time.Duration:
		return NewDurationValue(v), nil
	case nil:
		return NewNullValue(), nil
	case Document:
		return NewDocumentValue(v), nil
	case Array:
		return NewArrayValue(v), nil
	case int:
		return NewIntegerValue(int64(v)), nil
	case bool:
		return NewBoolValue(v), nil
	case float64:
		return NewDoubleValue(v), nil
	case string:
		return NewTextValue(v), nil
	}

	return Value{}, &ErrUnsupportedType{x, ""}
}
