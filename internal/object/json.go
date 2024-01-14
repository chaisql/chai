package object

import (
	"github.com/buger/jsonparser"
	"github.com/chaisql/chai/internal/types"
)

func parseJSONValue(dataType jsonparser.ValueType, data []byte) (v types.Value, err error) {
	switch dataType {
	case jsonparser.Null:
		return types.NewNullValue(), nil
	case jsonparser.Boolean:
		b, err := jsonparser.ParseBoolean(data)
		if err != nil {
			return nil, err
		}
		return types.NewBooleanValue(b), nil
	case jsonparser.Number:
		i, err := jsonparser.ParseInt(data)
		if err != nil {
			// if it's too big to fit in an int64, let's try parsing this as a floating point number
			f, err := jsonparser.ParseFloat(data)
			if err != nil {
				return nil, err
			}

			return types.NewDoubleValue(f), nil
		}

		return types.NewIntegerValue(i), nil
	case jsonparser.String:
		s, err := jsonparser.ParseString(data)
		if err != nil {
			return nil, err
		}
		return types.NewTextValue(s), nil
	case jsonparser.Array:
		buf := NewValueBuffer()
		err := buf.UnmarshalJSON(data)
		if err != nil {
			return nil, err
		}

		return types.NewArrayValue(buf), nil
	case jsonparser.Object:
		buf := NewFieldBuffer()
		err = buf.UnmarshalJSON(data)
		if err != nil {
			return nil, err
		}

		return types.NewObjectValue(buf), nil
	default:
	}

	return nil, nil
}
