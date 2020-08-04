package document

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"time"
)

// CastAs casts v as the selected type when possible.
func (v Value) CastAs(t ValueType) (Value, error) {
	if v.Type == t {
		return v, nil
	}

	// Null values always remain null.
	if v.Type == NullValue {
		return v, nil
	}

	switch t {
	case BoolValue:
		return v.CastAsBool()
	case IntegerValue:
		return v.CastAsInteger()
	case DoubleValue:
		return v.CastAsDouble()
	case DurationValue:
		return v.CastAsDuration()
	case BlobValue:
		return v.CastAsBlob()
	case TextValue:
		return v.CastAsText()
	case ArrayValue:
		return v.CastAsArray()
	case DocumentValue:
		return v.CastAsDocument()
	}

	return Value{}, fmt.Errorf("cannot cast %s as %q", v.Type, t)
}

// CastAsBool casts according to the following rules:
// Integer: true if truthy, otherwise false.
// Text: uses strconv.Parsebool to determine the boolean value,
// it fails if the text doesn't contain a valid boolean.
// Any other type is considered an invalid cast.
func (v Value) CastAsBool() (Value, error) {
	switch v.Type {
	case BoolValue:
		return v, nil
	case IntegerValue:
		return NewBoolValue(v.V.(int64) != 0), nil
	case TextValue:
		b, err := strconv.ParseBool(string(v.V.([]byte)))
		if err != nil {
			return Value{}, fmt.Errorf(`cannot cast %q as bool: %w`, v.V, err)
		}
		return NewBoolValue(b), nil
	}

	return Value{}, fmt.Errorf("cannot cast %s as bool", v.Type)
}

// CastAsInteger casts according to the following rules:
// Bool: returns 1 if true, 0 if false.
// Double: cuts off the decimal and remaining numbers.
// Duration: returns the number of nanoseconds in the duration.
// Text: uses strconv.ParseFloat to determine the double value,
// then casts it to an integer. It fails if the text doesn't
// contain a valid float value.
// Any other type is considered an invalid cast.
func (v Value) CastAsInteger() (Value, error) {
	switch v.Type {
	case IntegerValue:
		return v, nil
	case BoolValue:
		if v.V.(bool) {
			return NewIntegerValue(1), nil
		}
		return NewIntegerValue(0), nil
	case DoubleValue:
		return NewIntegerValue(int64(v.V.(float64))), nil
	case DurationValue:
		return NewIntegerValue(int64(v.V.(time.Duration))), nil
	case TextValue:
		f, err := strconv.ParseFloat(string(v.V.([]byte)), 64)
		if err != nil {
			return Value{}, fmt.Errorf(`cannot cast %q as integer: %w`, v.V, err)
		}
		return NewIntegerValue(int64(f)), nil
	}

	return Value{}, fmt.Errorf("cannot cast %s as integer", v.Type)
}

// CastAsDouble casts according to the following rules:
// Integer: returns a double version of the integer.
// Text: uses strconv.ParseFloat to determine the double value,
// it fails if the text doesn't contain a valid float value.
// Any other type is considered an invalid cast.
func (v Value) CastAsDouble() (Value, error) {
	switch v.Type {
	case DoubleValue:
		return v, nil
	case IntegerValue:
		return NewDoubleValue(float64(v.V.(int64))), nil
	case TextValue:
		f, err := strconv.ParseFloat(string(v.V.([]byte)), 64)
		if err != nil {
			return Value{}, fmt.Errorf(`cannot cast %q as double: %w`, v.V, err)
		}
		return NewDoubleValue(f), nil
	}

	return Value{}, fmt.Errorf("cannot cast %s as double", v.Type)
}

// CastAsDuration casts according to the following rules:
// Text: decodes using time.ParseDuration, otherwise fails.
// Any other type is considered an invalid cast.
func (v Value) CastAsDuration() (Value, error) {
	if v.Type == DurationValue {
		return v, nil
	}

	if v.Type == TextValue {
		d, err := time.ParseDuration(string(v.V.([]byte)))
		if err != nil {
			return Value{}, fmt.Errorf(`cannot cast %q as duration: %w`, v.V, err)
		}

		return NewDurationValue(d), nil
	}

	return Value{}, fmt.Errorf("cannot cast %s as duration", v.Type)
}

// CastAsText returns a JSON representation of v.
// If the representation is a string, it gets unquoted.
func (v Value) CastAsText() (Value, error) {
	if v.Type == TextValue {
		return v, nil
	}

	d, err := v.MarshalJSON()
	if err != nil {
		return Value{}, err
	}

	s := string(d)

	if v.Type == DurationValue || v.Type == BlobValue {
		s, err = strconv.Unquote(s)
		if err != nil {
			return Value{}, err
		}
	}

	return NewTextValue(s), nil
}

// CastAsBlob casts according to the following rules:
// Text: decodes a base64 string, otherwise fails.
// Any other type is considered an invalid cast.
func (v Value) CastAsBlob() (Value, error) {
	if v.Type == BlobValue {
		return v, nil
	}

	if v.Type == TextValue {
		b, err := base64.StdEncoding.DecodeString(string(v.V.([]byte)))
		if err != nil {
			return Value{}, fmt.Errorf(`cannot cast %q as blob: %w`, v.V, err)
		}

		return NewBlobValue(b), nil
	}

	return Value{}, fmt.Errorf("cannot cast %s as blob", v.Type)
}

// CastAsArray casts according to the following rules:
// Text: decodes a JSON array, otherwise fails.
// Any other type is considered an invalid cast.
func (v Value) CastAsArray() (Value, error) {
	if v.Type == ArrayValue {
		return v, nil
	}

	if v.Type == TextValue {
		var vb ValueBuffer
		err := vb.UnmarshalJSON([]byte(string(v.V.([]byte))))
		if err != nil {
			return Value{}, fmt.Errorf(`cannot cast %q as array: %w`, v.V, err)
		}

		return NewArrayValue(vb), nil
	}

	return Value{}, fmt.Errorf("cannot cast %s as array", v.Type)
}

// CastAsDocument casts according to the following rules:
// Text: decodes a JSON object, otherwise fails.
// Any other type is considered an invalid cast.
func (v Value) CastAsDocument() (Value, error) {
	if v.Type == DocumentValue {
		return v, nil
	}

	if v.Type == TextValue {
		var fb FieldBuffer
		err := fb.UnmarshalJSON([]byte(string(v.V.([]byte))))
		if err != nil {
			return Value{}, fmt.Errorf(`cannot cast %q as document: %w`, v.V, err)
		}

		return NewDocumentValue(&fb), nil
	}

	return Value{}, fmt.Errorf("cannot cast %s as document", v.Type)
}
