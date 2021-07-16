package types

import (
	"bytes"
	"encoding/base64"
	"errors"
	"math"
	"strconv"

	"github.com/genjidb/genji/internal/binarysort"
	"github.com/genjidb/genji/internal/stringutil"
)

type Value interface {
	Type() ValueType
	V() interface{}
	// TODO(asdine): Remove the following methods from
	// this interface and use type inference instead.
	MarshalJSON() ([]byte, error)
	MarshalBinary() ([]byte, error)
	String() string
}

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

// MarshalJSON implements the json.Marshaler interface.
func (v *value) MarshalJSON() ([]byte, error) {
	switch v.tp {
	case NullValue:
		return []byte("null"), nil
	case BoolValue:
		return strconv.AppendBool(nil, v.v.(bool)), nil
	case IntegerValue:
		return strconv.AppendInt(nil, v.v.(int64), 10), nil
	case DoubleValue:
		f := v.v.(float64)
		abs := math.Abs(f)
		fmt := byte('f')
		if abs != 0 {
			if abs < 1e-6 || abs >= 1e21 {
				fmt = 'e'
			}
		}

		// By default the precision is -1 to use the smallest number of digits.
		// See https://pkg.go.dev/strconv#FormatFloat
		prec := -1

		return strconv.AppendFloat(nil, v.v.(float64), fmt, prec, 64), nil
	case TextValue:
		return []byte(strconv.Quote(v.v.(string))), nil
	case BlobValue:
		src := v.v.([]byte)
		dst := make([]byte, base64.StdEncoding.EncodedLen(len(src))+2)
		dst[0] = '"'
		dst[len(dst)-1] = '"'
		base64.StdEncoding.Encode(dst[1:], src)
		return dst, nil
	case ArrayValue:
		return JsonArray{v.v.(Array)}.MarshalJSON()
	case DocumentValue:
		return JsonDocument{v.v.(Document)}.MarshalJSON()
	default:
		return nil, stringutil.Errorf("unexpected type: %d", v.tp)
	}
}

// String returns a string representation of the value. It implements the fmt.Stringer interface.
func (v *value) String() string {
	switch v.tp {
	case NullValue:
		return "NULL"
	case TextValue:
		return strconv.Quote(v.v.(string))
	case BlobValue:
		return stringutil.Sprintf("%v", v.v)
	}

	d, _ := v.MarshalJSON()
	return string(d)
}

// Append appends to buf a binary representation of v.
// The encoded value doesn't include type information.
func (v *value) Append(buf []byte) ([]byte, error) {
	switch v.tp {
	case BlobValue:
		return append(buf, v.v.([]byte)...), nil
	case TextValue:
		return append(buf, v.v.(string)...), nil
	case BoolValue:
		return binarysort.AppendBool(buf, v.v.(bool)), nil
	case IntegerValue:
		return binarysort.AppendInt64(buf, v.v.(int64)), nil
	case DoubleValue:
		return binarysort.AppendFloat64(buf, v.v.(float64)), nil
	case NullValue:
		return buf, nil
	case ArrayValue:
		var buf bytes.Buffer
		err := NewValueEncoder(&buf).appendArray(v.v.(Array))
		if err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	case DocumentValue:
		var buf bytes.Buffer
		err := NewValueEncoder(&buf).appendDocument(v.v.(Document))
		if err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}

	return nil, errors.New("cannot encode type " + v.tp.String() + " as key")
}

// MarshalBinary returns a binary representation of v.
// The encoded value doesn't include type information.
func (v *value) MarshalBinary() ([]byte, error) {
	return v.Append(nil)
}
