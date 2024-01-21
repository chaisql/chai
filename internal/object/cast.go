package object

import (
	"encoding/base64"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/chaisql/chai/internal/types"
)

// CastAs casts v as the selected type when possible.
func CastAs(v types.Value, t types.ValueType) (types.Value, error) {
	if v.Type() == t {
		return v, nil
	}

	switch t {
	case types.TypeBoolean:
		return CastAsBool(v)
	case types.TypeInteger:
		return CastAsInteger(v)
	case types.TypeDouble:
		return CastAsDouble(v)
	case types.TypeTimestamp:
		return CastAsTimestamp(v)
	case types.TypeBlob:
		return CastAsBlob(v)
	case types.TypeText:
		return CastAsText(v)
	case types.TypeArray:
		return CastAsArray(v)
	case types.TypeObject:
		return CastAsObject(v)
	}

	return nil, fmt.Errorf("cannot cast %s as %q", v.Type(), t)
}

// CastAsBool casts according to the following rules:
// Integer: true if truthy, otherwise false.
// Text: uses strconv.Parsebool to determine the boolean value,
// it fails if the text doesn't contain a valid boolean.
// Any other type is considered an invalid cast.
func CastAsBool(v types.Value) (types.Value, error) {
	// Null values always remain null.
	if v.Type() == types.TypeNull {
		return v, nil
	}

	switch v.Type() {
	case types.TypeBoolean:
		return v, nil
	case types.TypeInteger:
		return types.NewBooleanValue(types.AsInt64(v) != 0), nil
	case types.TypeText:
		b, err := strconv.ParseBool(types.AsString(v))
		if err != nil {
			return nil, fmt.Errorf(`cannot cast %q as bool: %w`, v.V(), err)
		}
		return types.NewBooleanValue(b), nil
	}

	return nil, fmt.Errorf("cannot cast %s as bool", v.Type())
}

// CastAsInteger casts according to the following rules:
// Bool: returns 1 if true, 0 if false.
// Double: cuts off the decimal and remaining numbers.
// Text: uses strconv.ParseInt to determine the integer value,
// then casts it to an integer. If it fails uses strconv.ParseFloat
// to determine the double value, then casts it to an integer
// It fails if the text doesn't contain a valid float value.
// Any other type is considered an invalid cast.
func CastAsInteger(v types.Value) (types.Value, error) {
	// Null values always remain null.
	if v.Type() == types.TypeNull {
		return v, nil
	}

	switch v.Type() {
	case types.TypeInteger:
		return v, nil
	case types.TypeBoolean:
		if types.AsBool(v) {
			return types.NewIntegerValue(1), nil
		}
		return types.NewIntegerValue(0), nil
	case types.TypeDouble:
		f := types.AsFloat64(v)
		if f > 0 && (int64(f) < 0 || f >= math.MaxInt64) {
			return nil, fmt.Errorf("integer out of range")
		}
		return types.NewIntegerValue(int64(f)), nil
	case types.TypeText:
		i, err := strconv.ParseInt(types.AsString(v), 10, 64)
		if err != nil {
			intErr := err
			f, err := strconv.ParseFloat(types.AsString(v), 64)
			if err != nil {
				return nil, fmt.Errorf(`cannot cast %q as integer: %w`, v.V(), intErr)
			}
			i = int64(f)
		}
		return types.NewIntegerValue(i), nil
	}

	return nil, fmt.Errorf("cannot cast %s as integer", v.Type())
}

// CastAsDouble casts according to the following rules:
// Integer: returns a double version of the integer.
// Text: uses strconv.ParseFloat to determine the double value,
// it fails if the text doesn't contain a valid float value.
// Any other type is considered an invalid cast.
func CastAsDouble(v types.Value) (types.Value, error) {
	// Null values always remain null.
	if v.Type() == types.TypeNull {
		return v, nil
	}

	switch v.Type() {
	case types.TypeDouble:
		return v, nil
	case types.TypeInteger:
		return types.NewDoubleValue(float64(types.AsInt64(v))), nil
	case types.TypeText:
		f, err := strconv.ParseFloat(types.AsString(v), 64)
		if err != nil {
			return nil, fmt.Errorf(`cannot cast %q as double: %w`, v.V(), err)
		}
		return types.NewDoubleValue(f), nil
	}

	return nil, fmt.Errorf("cannot cast %s as double", v.Type())
}

// CastAsTimestamp casts according to the following rules:
// Text: uses carbon.Parse to determine the timestamp value
// it fails if the text doesn't contain a valid timestamp.
// Any other type is considered an invalid cast.
func CastAsTimestamp(v types.Value) (types.Value, error) {
	// Null values always remain null.
	if v.Type() == types.TypeNull {
		return v, nil
	}

	switch v.Type() {
	case types.TypeTimestamp:
		return v, nil
	case types.TypeText:
		t, err := types.ParseTimestamp(types.AsString(v))
		if err != nil {
			return nil, fmt.Errorf(`cannot cast %q as timestamp: %w`, v.V(), err)
		}
		return types.NewTimestampValue(t), nil
	}

	return nil, fmt.Errorf("cannot cast %s as timestamp", v.Type())
}

// CastAsText returns a JSON representation of v.
// If the representation is a string, it gets unquoted.
func CastAsText(v types.Value) (types.Value, error) {
	// Null values always remain null.
	if v.Type() == types.TypeNull {
		return v, nil
	}

	switch v.Type() {
	case types.TypeText:
		return v, nil
	case types.TypeBlob:
		return types.NewTextValue(base64.StdEncoding.EncodeToString(types.AsByteSlice(v))), nil
	case types.TypeTimestamp:
		return types.NewTextValue(types.AsTime(v).Format(time.RFC3339Nano)), nil
	}

	d, err := v.MarshalJSON()
	if err != nil {
		return nil, err
	}

	s := string(d)

	return types.NewTextValue(s), nil
}

// CastAsBlob casts according to the following rules:
// Text: decodes a base64 string, otherwise fails.
// Any other type is considered an invalid cast.
func CastAsBlob(v types.Value) (types.Value, error) {
	// Null values always remain null.
	if v.Type() == types.TypeNull {
		return v, nil
	}

	if v.Type() == types.TypeBlob {
		return v, nil
	}

	if v.Type() == types.TypeText {
		// if the string starts with \x, read it as hex
		s := types.AsString(v)
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, err
		}

		return types.NewBlobValue(b), nil
	}

	return nil, fmt.Errorf("cannot cast %s as blob", v.Type())
}

// CastAsArray casts according to the following rules:
// Text: decodes a JSON array, otherwise fails.
// Any other type is considered an invalid cast.
func CastAsArray(v types.Value) (types.Value, error) {
	// Null values always remain null.
	if v.Type() == types.TypeNull {
		return v, nil
	}

	if v.Type() == types.TypeArray {
		return v, nil
	}

	if v.Type() == types.TypeText {
		var vb ValueBuffer
		err := vb.UnmarshalJSON([]byte(types.AsString(v)))
		if err != nil {
			return nil, fmt.Errorf(`cannot cast %q as array: %w`, v.V(), err)
		}

		return types.NewArrayValue(&vb), nil
	}

	return nil, fmt.Errorf("cannot cast %s as array", v.Type())
}

// CastAsObject casts according to the following rules:
// Text: decodes a JSON object, otherwise fails.
// Any other type is considered an invalid cast.
func CastAsObject(v types.Value) (types.Value, error) {
	// Null values always remain null.
	if v.Type() == types.TypeNull {
		return v, nil
	}

	if v.Type() == types.TypeObject {
		return v, nil
	}

	if v.Type() == types.TypeText {
		var fb FieldBuffer
		err := fb.UnmarshalJSON([]byte(types.AsString(v)))
		if err != nil {
			return nil, fmt.Errorf(`cannot cast %q as object: %w`, v.V(), err)
		}

		return types.NewObjectValue(&fb), nil
	}

	return nil, fmt.Errorf("cannot cast %s as object", v.Type())
}
