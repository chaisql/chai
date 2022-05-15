package document

import (
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/genjidb/genji/types"
)

// CastAs casts v as the selected type when possible.
func CastAs(v types.Value, t types.ValueType) (types.Value, error) {
	if v.Type() == t {
		return v, nil
	}

	switch t {
	case types.BooleanValue:
		return CastAsBool(v)
	case types.IntegerValue:
		return CastAsInteger(v)
	case types.DoubleValue:
		return CastAsDouble(v)
	case types.BlobValue:
		return CastAsBlob(v)
	case types.TextValue:
		return CastAsText(v)
	case types.ArrayValue:
		return CastAsArray(v)
	case types.DocumentValue:
		return CastAsDocument(v)
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
	if v.Type() == types.NullValue {
		return v, nil
	}

	switch v.Type() {
	case types.BooleanValue:
		return v, nil
	case types.IntegerValue:
		return types.NewBoolValue(types.As[int64](v) != 0), nil
	case types.TextValue:
		b, err := strconv.ParseBool(types.As[string](v))
		if err != nil {
			return nil, fmt.Errorf(`cannot cast %q as bool: %w`, v.V(), err)
		}
		return types.NewBoolValue(b), nil
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
	if v.Type() == types.NullValue {
		return v, nil
	}

	switch v.Type() {
	case types.IntegerValue:
		return v, nil
	case types.BooleanValue:
		if types.As[bool](v) {
			return types.NewIntegerValue(1), nil
		}
		return types.NewIntegerValue(0), nil
	case types.DoubleValue:
		f := types.As[float64](v)
		if f > 0 && int64(f) < 0 {
			return nil, fmt.Errorf("integer out of range")
		}
		return types.NewIntegerValue(int64(f)), nil
	case types.TextValue:
		i, err := strconv.ParseInt(types.As[string](v), 10, 64)
		if err != nil {
			intErr := err
			f, err := strconv.ParseFloat(types.As[string](v), 64)
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
	if v.Type() == types.NullValue {
		return v, nil
	}

	switch v.Type() {
	case types.DoubleValue:
		return v, nil
	case types.IntegerValue:
		return types.NewDoubleValue(float64(types.As[int64](v))), nil
	case types.TextValue:
		f, err := strconv.ParseFloat(types.As[string](v), 64)
		if err != nil {
			return nil, fmt.Errorf(`cannot cast %q as double: %w`, v.V(), err)
		}
		return types.NewDoubleValue(f), nil
	}

	return nil, fmt.Errorf("cannot cast %s as double", v.Type())
}

// CastAsText returns a JSON representation of v.
// If the representation is a string, it gets unquoted.
func CastAsText(v types.Value) (types.Value, error) {
	// Null values always remain null.
	if v.Type() == types.NullValue {
		return v, nil
	}

	switch v.Type() {
	case types.TextValue:
		return v, nil
	case types.BlobValue:
		return types.NewTextValue(base64.StdEncoding.EncodeToString(types.As[[]byte](v))), nil
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
	if v.Type() == types.NullValue {
		return v, nil
	}

	if v.Type() == types.BlobValue {
		return v, nil
	}

	if v.Type() == types.TextValue {
		// if the string starts with \x, read it as hex
		s := types.As[string](v)
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
	if v.Type() == types.NullValue {
		return v, nil
	}

	if v.Type() == types.ArrayValue {
		return v, nil
	}

	if v.Type() == types.TextValue {
		var vb ValueBuffer
		err := vb.UnmarshalJSON([]byte(types.As[string](v)))
		if err != nil {
			return nil, fmt.Errorf(`cannot cast %q as array: %w`, v.V(), err)
		}

		return types.NewArrayValue(&vb), nil
	}

	return nil, fmt.Errorf("cannot cast %s as array", v.Type())
}

// CastAsDocument casts according to the following rules:
// Text: decodes a JSON object, otherwise fails.
// Any other type is considered an invalid cast.
func CastAsDocument(v types.Value) (types.Value, error) {
	// Null values always remain null.
	if v.Type() == types.NullValue {
		return v, nil
	}

	if v.Type() == types.DocumentValue {
		return v, nil
	}

	if v.Type() == types.TextValue {
		var fb FieldBuffer
		err := fb.UnmarshalJSON([]byte(types.As[string](v)))
		if err != nil {
			return nil, fmt.Errorf(`cannot cast %q as document: %w`, v.V(), err)
		}

		return types.NewDocumentValue(&fb), nil
	}

	return nil, fmt.Errorf("cannot cast %s as document", v.Type())
}
