package document

import "github.com/genjidb/genji/types"

// NewValue creates a value from x. It only supports a few types and doesn't rely on reflection.
func NewValue(x interface{}) (types.Value, error) {
	switch v := x.(type) {
	case nil:
		return types.NewNullValue(), nil
	case types.Document:
		return types.NewDocumentValue(v), nil
	case types.Array:
		return types.NewArrayValue(v), nil
	case int:
		return types.NewIntegerValue(int64(v)), nil
	case bool:
		return types.NewBoolValue(v), nil
	case float64:
		return types.NewDoubleValue(v), nil
	case string:
		return types.NewTextValue(v), nil
	}

	return nil, &ErrUnsupportedType{x, ""}
}
