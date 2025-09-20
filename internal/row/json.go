package row

import (
	"math"

	"github.com/buger/jsonparser"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
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

			return types.NewDoublePrevisionValue(f), nil
		}

		if i < math.MinInt32 || i > math.MaxInt32 {
			return types.NewBigintValue(i), nil
		}

		return types.NewIntegerValue(int32(i)), nil
	case jsonparser.String:
		s, err := jsonparser.ParseString(data)
		if err != nil {
			return nil, err
		}
		return types.NewTextValue(s), nil
	default:
		return nil, errors.Errorf("unsupported JSON type: %v", dataType)
	}
}
