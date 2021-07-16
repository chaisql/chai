package document

import (
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/types"
)

// CastAs casts v as the selected type when possible.
func CastAs(v types.Value, t types.ValueType) (types.Value, error) {
	if v.Type() == t {
		return v, nil
	}

	// Null values always remain null.
	if v.Type() == types.NullValue {
		return v, nil
	}

	switch t {
	case types.BoolValue:
		return types.CastAsBool(v)
	case types.IntegerValue:
		return types.CastAsInteger(v)
	case types.DoubleValue:
		return types.CastAsDouble(v)
	case types.BlobValue:
		return types.CastAsBlob(v)
	case types.TextValue:
		return types.CastAsText(v)
	case types.ArrayValue:
		return CastAsArray(v)
	case types.DocumentValue:
		return CastAsDocument(v)
	}

	return nil, stringutil.Errorf("cannot cast %s as %q", v.Type(), t)
}

// CastAsArray casts according to the following rules:
// Text: decodes a JSON array, otherwise fails.
// Any other type is considered an invalid cast.
func CastAsArray(v types.Value) (types.Value, error) {
	if v.Type() == types.ArrayValue {
		return v, nil
	}

	if v.Type() == types.TextValue {
		var vb ValueBuffer
		err := vb.UnmarshalJSON([]byte(v.V().(string)))
		if err != nil {
			return nil, stringutil.Errorf(`cannot cast %q as array: %w`, v.V(), err)
		}

		return types.NewArrayValue(&vb), nil
	}

	return nil, stringutil.Errorf("cannot cast %s as array", v.Type())
}

// CastAsDocument casts according to the following rules:
// Text: decodes a JSON object, otherwise fails.
// Any other type is considered an invalid cast.
func CastAsDocument(v types.Value) (types.Value, error) {
	if v.Type() == types.DocumentValue {
		return v, nil
	}

	if v.Type() == types.TextValue {
		var fb FieldBuffer
		err := fb.UnmarshalJSON([]byte(v.V().(string)))
		if err != nil {
			return nil, stringutil.Errorf(`cannot cast %q as document: %w`, v.V(), err)
		}

		return types.NewDocumentValue(&fb), nil
	}

	return nil, stringutil.Errorf("cannot cast %s as document", v.Type())
}
