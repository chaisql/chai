package types

// A Value stores encoded data alongside its type.
type value struct {
	tp ValueType
	v  interface{}
}

var _ Value = &value{}

// NewNullValue returns a Null value.
func NewNullValue() Value {
	return &value{
		tp: NullValue,
	}
}

// NewBoolValue encodes x and returns a value.
func NewBoolValue(x bool) Value {
	return &value{
		tp: BoolValue,
		v:  x,
	}
}

// NewIntegerValue encodes x and returns a value whose type depends on the
// magnitude of x.
func NewIntegerValue(x int64) Value {
	return &value{
		tp: IntegerValue,
		v:  int64(x),
	}
}

// NewDoubleValue encodes x and returns a value.
func NewDoubleValue(x float64) Value {
	return &value{
		tp: DoubleValue,
		v:  x,
	}
}

// NewBlobValue encodes x and returns a value.
func NewBlobValue(x []byte) Value {
	return &value{
		tp: BlobValue,
		v:  x,
	}
}

// NewTextValue encodes x and returns a value.
func NewTextValue(x string) Value {
	return &value{
		tp: TextValue,
		v:  x,
	}
}

// NewArrayValue returns a value of type Array.
func NewArrayValue(a Array) Value {
	return &value{
		tp: ArrayValue,
		v:  a,
	}
}

// NewDocumentValue returns a value of type Document.
func NewDocumentValue(d Document) Value {
	return &value{
		tp: DocumentValue,
		v:  d,
	}
}

// NewEmptyValue creates an empty value with the given type.
// V() always returns nil.
func NewEmptyValue(t ValueType) Value {
	return &value{
		tp: t,
	}
}

// NewValueWith creates a value with the given type and value.
func NewValueWith(t ValueType, v interface{}) Value {
	return &value{
		tp: t,
		v:  v,
	}
}

func (v *value) V() interface{} {
	return v.v
}

func (v *value) Type() ValueType {
	return v.tp
}

// IsTruthy returns whether v is not equal to the zero value of its type.
func IsTruthy(v Value) (bool, error) {
	if v.Type() == NullValue {
		return false, nil
	}

	b, err := IsZeroValue(v)
	return !b, err
}

// IsZeroValue indicates if the value data is the zero value for the value type.
// This function doesn't perform any allocation.
func IsZeroValue(v Value) (bool, error) {
	switch v.Type() {
	case BoolValue:
		return v.V() == false, nil
	case IntegerValue:
		return v.V() == int64(0), nil
	case DoubleValue:
		return v.V() == float64(0), nil
	case BlobValue:
		return v.V() == nil, nil
	case TextValue:
		return v.V() == "", nil
	case ArrayValue:
		// The zero value of an array is an empty array.
		// Thus, if GetByIndex(0) returns the ErrValueNotFound
		// it means that the array is empty.
		_, err := v.V().(Array).GetByIndex(0)
		if err == ErrValueNotFound {
			return true, nil
		}
		return false, err
	case DocumentValue:
		err := v.V().(Document).Iterate(func(_ string, _ Value) error {
			// We return an error in the first iteration to stop it.
			return errStop
		})
		if err == nil {
			// If err is nil, it means that we didn't iterate,
			// thus the document is empty.
			return true, nil
		}
		if err == errStop {
			// If err is errStop, it means that we iterate
			// at least once, thus the document is not empty.
			return false, nil
		}
		// An unexpecting error occurs, let's return it!
		return false, err
	}

	return false, nil
}
