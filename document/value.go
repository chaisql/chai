package document

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/buger/jsonparser"
)

var (
	boolZeroValue     = NewZeroValue(BoolValue)
	integerZeroValue  = NewZeroValue(IntegerValue)
	doubleZeroValue   = NewZeroValue(DoubleValue)
	blobZeroValue     = NewZeroValue(BlobValue)
	textZeroValue     = NewZeroValue(TextValue)
	arrayZeroValue    = NewZeroValue(ArrayValue)
	documentZeroValue = NewZeroValue(DocumentValue)
)

// ErrUnsupportedType is used to skip struct or array fields that are not supported.
type ErrUnsupportedType struct {
	Value interface{}
	Msg   string
}

func (e *ErrUnsupportedType) Error() string {
	return fmt.Sprintf("unsupported type %T. %s", e.Value, e.Msg)
}

// ValueType represents a value type supported by the database.
type ValueType uint8

// List of supported value types.
// These types are separated by family so that when
// new types are introduced we don't need to modify them.
const (
	NullValue ValueType = 0x80

	BoolValue ValueType = 0x81

	// integer family: 0x90 to 0x9F
	IntegerValue ValueType = 0x90

	// double family: 0xA0 to 0xAF
	DoubleValue ValueType = 0xA0

	// string family: 0xC0 to 0xCF
	TextValue ValueType = 0xC0

	// blob family: 0xD0 to 0xDF
	BlobValue ValueType = 0xD0

	// array family: 0xE0 to 0xEF
	ArrayValue ValueType = 0xE0

	// document family: 0xF0 to 0xFF
	DocumentValue ValueType = 0xF0
)

func (t ValueType) String() string {
	switch t {
	case NullValue:
		return "null"
	case BoolValue:
		return "bool"
	case IntegerValue:
		return "integer"
	case DoubleValue:
		return "double"
	case BlobValue:
		return "blob"
	case TextValue:
		return "text"
	case ArrayValue:
		return "array"
	case DocumentValue:
		return "document"
	}

	return ""
}

// IsNumber returns true if t is either an integer of a float.
func (t ValueType) IsNumber() bool {
	return t == IntegerValue || t == DoubleValue
}

// A Value stores encoded data alongside its type.
type Value struct {
	Type ValueType
	V    interface{}
}

// NewNullValue returns a Null value.
func NewNullValue() Value {
	return Value{
		Type: NullValue,
	}
}

// NewBoolValue encodes x and returns a value.
func NewBoolValue(x bool) Value {
	return Value{
		Type: BoolValue,
		V:    x,
	}
}

// NewIntegerValue encodes x and returns a value whose type depends on the
// magnitude of x.
func NewIntegerValue(x int64) Value {
	return Value{
		Type: IntegerValue,
		V:    int64(x),
	}
}

// NewDoubleValue encodes x and returns a value.
func NewDoubleValue(x float64) Value {
	return Value{
		Type: DoubleValue,
		V:    x,
	}
}

// NewBlobValue encodes x and returns a value.
func NewBlobValue(x []byte) Value {
	return Value{
		Type: BlobValue,
		V:    x,
	}
}

// NewTextValue encodes x and returns a value.
func NewTextValue(x string) Value {
	return Value{
		Type: TextValue,
		V:    x,
	}
}

// NewArrayValue returns a value of type Array.
func NewArrayValue(a Array) Value {
	return Value{
		Type: ArrayValue,
		V:    a,
	}
}

// NewDocumentValue returns a value of type Document.
func NewDocumentValue(d Document) Value {
	return Value{
		Type: DocumentValue,
		V:    d,
	}
}

// NewZeroValue returns a value whose value is equal to the Go zero value
// of the selected type.
func NewZeroValue(t ValueType) Value {
	switch t {
	case NullValue:
		return NewNullValue()
	case BoolValue:
		return NewBoolValue(false)
	case IntegerValue:
		return NewIntegerValue(0)
	case DoubleValue:
		return NewDoubleValue(0)
	case BlobValue:
		return NewBlobValue(nil)
	case TextValue:
		return NewTextValue("")
	case ArrayValue:
		return NewArrayValue(NewValueBuffer())
	case DocumentValue:
		return NewDocumentValue(NewFieldBuffer())
	}

	return Value{}
}

// IsTruthy returns whether v is not equal to the zero value of its type.
func (v Value) IsTruthy() (bool, error) {
	if v.Type == NullValue {
		return false, nil
	}

	b, err := v.IsZeroValue()
	return !b, err
}

// IsZeroValue indicates if the value data is the zero value for the value type.
// This function doesn't perform any allocation.
func (v Value) IsZeroValue() (bool, error) {
	switch v.Type {
	case BoolValue:
		return v.V == boolZeroValue.V, nil
	case IntegerValue:
		return v.V == integerZeroValue.V, nil
	case DoubleValue:
		return v.V == doubleZeroValue.V, nil
	case BlobValue:
		return bytes.Compare(v.V.([]byte), blobZeroValue.V.([]byte)) == 0, nil
	case TextValue:
		return v.V == textZeroValue.V, nil
	case ArrayValue:
		// The zero value of an array is an empty array.
		// Thus, if GetByIndex(0) returns the ErrValueNotFound
		// it means that the array is empty.
		_, err := v.V.(Array).GetByIndex(0)
		if err == ErrValueNotFound {
			return true, nil
		}
		return false, err
	case DocumentValue:
		err := v.V.(Document).Iterate(func(_ string, _ Value) error {
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
func (v Value) MarshalJSON() ([]byte, error) {
	switch v.Type {
	case NullValue:
		return []byte("null"), nil
	case BoolValue:
		return strconv.AppendBool(nil, v.V.(bool)), nil
	case IntegerValue:
		return strconv.AppendInt(nil, v.V.(int64), 10), nil
	case DoubleValue:
		f := v.V.(float64)
		if f == float64(int(f)) {
			return strconv.AppendFloat(nil, f, 'f', 1, 64), nil
		}
		return strconv.AppendFloat(nil, f, 'f', -1, 64), nil
	case TextValue:
		return []byte(strconv.Quote(v.V.(string))), nil
	case BlobValue:
		src := v.V.([]byte)
		dst := make([]byte, base64.StdEncoding.EncodedLen(len(src))+2)
		dst[0] = '"'
		dst[len(dst)-1] = '"'
		base64.StdEncoding.Encode(dst[1:], src)
		return dst, nil
	case ArrayValue:
		return jsonArray{v.V.(Array)}.MarshalJSON()
	case DocumentValue:
		return jsonDocument{v.V.(Document)}.MarshalJSON()
	default:
		return nil, errors.New("unexpected type: " + v.Type.String())
	}
}

// String returns a string representation of the value. It implements the fmt.Stringer interface.
func (v Value) String() string {
	switch v.Type {
	case NullValue:
		return "NULL"
	case TextValue:
		return strconv.Quote(v.V.(string))
	case BlobValue:
		return fmt.Sprintf("%v", v.V)
	}

	d, _ := v.MarshalJSON()
	return string(d)
}

// Add u to v and return the result.
// Only numeric values and booleans can be added together.
func (v Value) Add(u Value) (res Value, err error) {
	return calculateValues(v, u, '+')
}

// Sub calculates v - u and returns the result.
// Only numeric values and booleans can be calculated together.
func (v Value) Sub(u Value) (res Value, err error) {
	return calculateValues(v, u, '-')
}

// Mul calculates v * u and returns the result.
// Only numeric values and booleans can be calculated together.
func (v Value) Mul(u Value) (res Value, err error) {
	return calculateValues(v, u, '*')
}

// Div calculates v / u and returns the result.
// Only numeric values and booleans can be calculated together.
// If both v and u are integers, the result will be an integer.
func (v Value) Div(u Value) (res Value, err error) {
	return calculateValues(v, u, '/')
}

// Mod calculates v / u and returns the result.
// Only numeric values and booleans can be calculated together.
// If both v and u are integers, the result will be an integer.
func (v Value) Mod(u Value) (res Value, err error) {
	return calculateValues(v, u, '%')
}

// BitwiseAnd calculates v & u and returns the result.
// Only numeric values and booleans can be calculated together.
// If both v and u are integers, the result will be an integer.
func (v Value) BitwiseAnd(u Value) (res Value, err error) {
	return calculateValues(v, u, '&')
}

// BitwiseOr calculates v | u and returns the result.
// Only numeric values and booleans can be calculated together.
// If both v and u are integers, the result will be an integer.
func (v Value) BitwiseOr(u Value) (res Value, err error) {
	return calculateValues(v, u, '|')
}

// BitwiseXor calculates v ^ u and returns the result.
// Only numeric values and booleans can be calculated together.
// If both v and u are integers, the result will be an integer.
func (v Value) BitwiseXor(u Value) (res Value, err error) {
	return calculateValues(v, u, '^')
}

func calculateValues(a, b Value, operator byte) (res Value, err error) {
	if a.Type == NullValue || b.Type == NullValue {
		return NewNullValue(), nil
	}

	if a.Type == BoolValue || b.Type == BoolValue {
		return NewNullValue(), nil
	}

	if a.Type.IsNumber() && b.Type.IsNumber() {
		if a.Type == DoubleValue || b.Type == DoubleValue {
			return calculateFloats(a, b, operator)
		}

		if a.Type == IntegerValue || b.Type == IntegerValue {
			return calculateIntegers(a, b, operator)
		}
	}

	return NewNullValue(), nil
}

func convertNumberToInt64(v Value) (int64, error) {
	var i int64

	switch v.Type {
	case IntegerValue:
		return v.V.(int64), nil
	case DoubleValue:
		f := v.V.(float64)
		if f > math.MaxInt64 {
			return i, errors.New(`cannot convert "double" to "integer" without overflowing`)
		}
		if math.Trunc(f) != f {
			return 0, errors.New(`cannot convert "double" value to "integer" without loss of precision`)
		}
		i = int64(f)
	}

	return i, nil
}

func calculateIntegers(a, b Value, operator byte) (res Value, err error) {
	var xa, xb int64

	ia, err := a.CastAsInteger()
	if err != nil {
		return NewNullValue(), nil
	}
	xa = ia.V.(int64)

	ib, err := b.CastAsInteger()
	if err != nil {
		return NewNullValue(), nil
	}
	xb = ib.V.(int64)

	var xr int64

	switch operator {
	case '-':
		xb = -xb
		fallthrough
	case '+':
		xr = xa + xb
		// if there is an integer overflow
		// convert to float
		if (xr > xa) != (xb > 0) {
			return NewDoubleValue(float64(xa) + float64(xb)), nil
		}
		return NewIntegerValue(xr), nil
	case '*':
		if xa == 0 || xb == 0 {
			return NewIntegerValue(0), nil
		}

		xr = xa * xb
		// if there is no integer overflow
		// return an int, otherwise
		// convert to float
		if (xr < 0) == ((xa < 0) != (xb < 0)) {
			if xr/xb == xa {
				return NewIntegerValue(xr), nil
			}
		}
		return NewDoubleValue(float64(xa) * float64(xb)), nil
	case '/':
		if xb == 0 {
			return NewNullValue(), nil
		}

		return NewIntegerValue(xa / xb), nil
	case '%':
		if xb == 0 {
			return NewNullValue(), nil
		}

		return NewIntegerValue(xa % xb), nil
	case '&':
		return NewIntegerValue(xa & xb), nil
	case '|':
		return NewIntegerValue(xa | xb), nil
	case '^':
		return NewIntegerValue(xa ^ xb), nil
	default:
		panic(fmt.Sprintf("unknown operator %c", operator))
	}
}

func calculateFloats(a, b Value, operator byte) (res Value, err error) {
	var xa, xb float64

	fa, err := a.CastAsDouble()
	if err != nil {
		return NewNullValue(), nil
	}
	xa = fa.V.(float64)

	fb, err := b.CastAsDouble()
	if err != nil {
		return NewNullValue(), nil
	}
	xb = fb.V.(float64)

	switch operator {
	case '+':
		return NewDoubleValue(xa + xb), nil
	case '-':
		return NewDoubleValue(xa - xb), nil
	case '*':
		return NewDoubleValue(xa * xb), nil
	case '/':
		if xb == 0 {
			return NewNullValue(), nil
		}

		return NewDoubleValue(xa / xb), nil
	case '%':
		if xb == 0 {
			return NewNullValue(), nil
		}

		ia, ib := int64(xa), int64(xb)
		return NewDoubleValue(float64(ia % ib)), nil
	case '&':
		ia, ib := int64(xa), int64(xb)
		return NewIntegerValue(ia & ib), nil
	case '|':
		ia, ib := int64(xa), int64(xb)
		return NewIntegerValue(ia | ib), nil
	case '^':
		ia, ib := int64(xa), int64(xb)
		return NewIntegerValue(ia ^ ib), nil
	default:
		panic(fmt.Sprintf("unknown operator %c", operator))
	}
}

func parseJSONValue(dataType jsonparser.ValueType, data []byte) (v Value, err error) {
	switch dataType {
	case jsonparser.Null:
		return NewNullValue(), nil
	case jsonparser.Boolean:
		b, err := jsonparser.ParseBoolean(data)
		if err != nil {
			return Value{}, err
		}
		return NewBoolValue(b), nil
	case jsonparser.Number:
		i, err := jsonparser.ParseInt(data)
		if err != nil {
			// if it's too big to fit in an int64, let's try parsing this as a floating point number
			f, err := jsonparser.ParseFloat(data)
			if err != nil {
				return Value{}, err
			}

			return NewDoubleValue(f), nil
		}

		return NewIntegerValue(i), nil
	case jsonparser.String:
		s, err := jsonparser.ParseString(data)
		if err != nil {
			return Value{}, err
		}
		return NewTextValue(s), nil
	case jsonparser.Array:
		buf := NewValueBuffer()
		err := buf.UnmarshalJSON(data)
		if err != nil {
			return Value{}, err
		}

		return NewArrayValue(buf), nil
	case jsonparser.Object:
		buf := NewFieldBuffer()
		err = buf.UnmarshalJSON(data)
		if err != nil {
			return Value{}, err
		}

		return NewDocumentValue(buf), nil
	default:
	}

	return Value{}, nil
}
