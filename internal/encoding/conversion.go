package encoding

import (
	"errors"

	"github.com/chaisql/chai/internal/object"
	"github.com/chaisql/chai/internal/types"
)

// ConvertFromStoreTo ensures the value read from the store is the same
// as the column type. Most types are stored as is but certain types are
// converted prior to being stored.
// Example: when there is no constraint on the column, integers are stored
// as doubles.
// The given target type is the type of the column, and it is used to determine
// whether there exists a constraint on the column or not.
func ConvertFromStoreTo(src types.Value, target types.Type) (types.Value, error) {
	if src.Type() == target {
		return src, nil
	}

	switch src.Type() {
	case types.TypeInteger:
		return convertIntegerFromStore(src, target)
	default:
		// if there is no constraint on the column, then the stored type
		// is the same as the runtime type.
		return src, nil
	}
}

func convertIntegerFromStore(src types.Value, target types.Type) (types.Value, error) {
	switch target {
	case types.TypeAny:
		return types.NewDoubleValue(float64(types.AsInt64(src))), nil
	case types.TypeTimestamp:
		return types.NewTimestampValue(ConvertToTimestamp(types.AsInt64(src))), nil
	}

	return nil, errors.New("cannot convert from store to " + target.String())
}

// ConvertAsStoreType converts the value to the type that is stored in the store
// when there is no constraint on the column.
func ConvertAsStoreType(src types.Value) (types.Value, error) {
	switch src.Type() {
	case types.TypeTimestamp:
		// without a type constraint, timestamp values must
		// always be stored as text to avoid mixed representations.
		return object.CastAsText(src)
	}

	return src, nil
}

// ConvertAsIndexType converts the value to the type that is stored in the index
// as a key.
func ConvertAsIndexType(src types.Value, target types.Type) (types.Value, error) {
	switch src.Type() {
	case types.TypeInteger:
		if target == types.TypeAny || target == types.TypeDouble {
			return object.CastAsDouble(src)
		}
		return src, nil
	case types.TypeTimestamp:
		// without a type constraint, timestamp values must
		// always be stored as text to avoid mixed representations.
		if target == types.TypeAny {
			return object.CastAsText(src)
		}
		return src, nil
	}

	return src, nil
}
