package document

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/genjidb/genji/pkg/bytesutil"
)

var (
	boolZeroValue     = NewZeroValue(BoolValue)
	integerZeroValue  = NewZeroValue(IntegerValue)
	doubleZeroValue   = NewZeroValue(DoubleValue)
	durationZeroValue = NewZeroValue(DurationValue)
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
	NullValue ValueType = 0x1

	BoolValue = 0x5

	// integer family: 0x10 to 0x1F
	IntegerValue = 0x10

	// double family: 0x20 to 0x2F
	DoubleValue = 0x20

	// time family: 0x30 to 0x3F
	DurationValue = 0x30

	// string family: 0x40 to 0x4F
	BlobValue = 0x40
	TextValue = 0x41

	// array family: 0x40 to 0x5F
	ArrayValue = 0x50

	// document family: 0x60 to 0x6F
	DocumentValue = 0x60
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
	case DurationValue:
		return "duration"
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
	return t.IsInteger() || t.IsFloat() || t == DurationValue
}

// IsInteger returns true if t is a signed or unsigned integer of any size.
func (t ValueType) IsInteger() bool {
	return t == IntegerValue || t == DurationValue
}

// IsFloat returns true if t is a Double.
func (t ValueType) IsFloat() bool {
	return t == DoubleValue
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

// NewDurationValue returns a value of type Duration.
func NewDurationValue(d time.Duration) Value {
	return Value{
		Type: DurationValue,
		V:    d,
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
		V:    []byte(x),
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
	case DurationValue:
		return NewDurationValue(0)
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

// ConvertTo decodes v to the selected type when possible.
func (v Value) ConvertTo(t ValueType) (Value, error) {
	if v.Type == t {
		return v, nil
	}

	// Null values always remain null.
	if v.Type == NullValue {
		return v, nil
	}

	switch t {
	case BoolValue:
		x, err := v.ConvertToBool()
		if err != nil {
			return Value{}, err
		}
		return NewBoolValue(x), nil
	case IntegerValue:
		x, err := v.ConvertToInt64()
		if err != nil {
			return Value{}, fmt.Errorf(`cannot convert %q to "integer": %w`, v.Type, err)
		}
		return NewIntegerValue(x), nil
	case DoubleValue:
		x, err := v.ConvertToFloat64()
		if err != nil {
			return Value{}, err
		}
		return NewDoubleValue(x), nil
	case DurationValue:
		x, err := v.ConvertToDuration()
		if err != nil {
			return Value{}, err
		}
		return NewDurationValue(x), nil
	case BlobValue:
		x, err := v.ConvertToBlob()
		if err != nil {
			return Value{}, err
		}
		return NewBlobValue(x), nil
	case TextValue:
		x, err := v.ConvertToText()
		if err != nil {
			return Value{}, err
		}
		return NewTextValue(x), nil
	}

	return Value{}, fmt.Errorf("cannot convert %q to %q", v.Type, t)
}

// ConvertToBlob converts a value of type Text or Blob to a slice of bytes.
// If fails if it's used with any other type.
func (v Value) ConvertToBlob() ([]byte, error) {
	switch v.Type {
	case TextValue, BlobValue:
		return v.V.([]byte), nil
	}

	if v.Type == NullValue {
		return nil, nil
	}

	return nil, fmt.Errorf(`cannot convert %q to "bytes"`, v.Type)
}

// ConvertToText turns a value of type Text or Blob into a string.
// If fails if it's used with any other type.
func (v Value) ConvertToText() (string, error) {
	switch v.Type {
	case TextValue, BlobValue:
		return string(v.V.([]byte)), nil
	}

	if v.Type == NullValue {
		return "", nil
	}

	return "", fmt.Errorf(`cannot convert %q to "string"`, v.Type)
}

// ConvertToBool returns true if v is truthy, otherwise it returns false.
func (v Value) ConvertToBool() (bool, error) {
	if v.Type == BoolValue {
		return v.V.(bool), nil
	}

	if v.Type == NullValue {
		return false, nil
	}

	b, err := v.IsZeroValue()
	return !b, err
}

// ConvertToInt64 turns any number into an int64.
// It doesn't work with other types.
func (v Value) ConvertToInt64() (int64, error) {
	if v.Type == IntegerValue {
		return v.V.(int64), nil
	}

	if v.Type == NullValue {
		return 0, nil
	}

	if v.Type.IsNumber() {
		return convertNumberToInt64(v)
	}

	if v.Type == BoolValue {
		if v.V.(bool) {
			return 1, nil
		}

		return 0, nil
	}

	return 0, fmt.Errorf(`type %q incompatible with "integer"`, v.Type)
}

// ConvertToFloat64 turns any number into a float64.
// It doesn't work with other types.
func (v Value) ConvertToFloat64() (float64, error) {
	if v.Type == DoubleValue {
		return v.V.(float64), nil
	}

	if v.Type == NullValue {
		return 0, nil
	}

	if v.Type.IsInteger() {
		x, err := convertNumberToInt64(v)
		if err != nil {
			return 0, err
		}
		return float64(x), nil
	}

	if v.Type == BoolValue {
		if v.V.(bool) {
			return 1, nil
		}

		return 0, nil
	}

	return 0, fmt.Errorf(`cannot convert %q to "double"`, v.Type)
}

// ConvertToDocument returns a document from the value.
// It only works if the type of v is DocumentValue.
func (v Value) ConvertToDocument() (Document, error) {
	if v.Type == NullValue {
		return NewFieldBuffer(), nil
	}

	if v.Type != DocumentValue {
		return nil, fmt.Errorf(`cannot convert %q to "document"`, v.Type)
	}

	return v.V.(Document), nil
}

// ConvertToArray returns an array from the value.
// It only works if the type of v is ArrayValue.
func (v Value) ConvertToArray() (Array, error) {
	if v.Type == NullValue {
		return NewValueBuffer(), nil
	}

	if v.Type != ArrayValue {
		return nil, fmt.Errorf(`cannot convert %q to "array"`, v.Type)
	}

	return v.V.(Array), nil
}

// ConvertToDuration turns any number into a time.Duration.
// It doesn't work with other types.
func (v Value) ConvertToDuration() (time.Duration, error) {
	if v.Type == DurationValue {
		return v.V.(time.Duration), nil
	}

	if v.Type == NullValue {
		return 0, nil
	}

	if v.Type == TextValue {
		d, err := time.ParseDuration(string(v.V.([]byte)))
		if err != nil {
			return 0, fmt.Errorf(`cannot convert %q to "duration": %v`, v.V, err)
		}
		return d, nil
	}

	x, err := v.ConvertToInt64()
	return time.Duration(x), err
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
	case DurationValue:
		return v.V == durationZeroValue.V, nil
	case BlobValue, TextValue:
		return bytesutil.CompareBytes(v.V.([]byte), blobZeroValue.V.([]byte)) == 0, nil
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

	if a.Type == BoolValue {
		a, err = a.ConvertTo(IntegerValue)
		if err != nil {
			return
		}
	}

	if b.Type == BoolValue {
		b, err = b.ConvertTo(IntegerValue)
		if err != nil {
			return
		}
	}

	if a.Type == DurationValue && b.Type == DurationValue {
		res, err = calculateIntegers(a, b, operator)
		if err != nil {
			return
		}
		if operator != '&' && operator != '|' && operator != '^' {
			return res.ConvertTo(DurationValue)
		}

		return
	}

	if a.Type.IsFloat() || b.Type.IsFloat() {
		return calculateFloats(a, b, operator)
	}

	if a.Type.IsInteger() || b.Type.IsInteger() {
		return calculateIntegers(a, b, operator)
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
	case DurationValue:
		return int64(v.V.(time.Duration)), nil
	}

	return i, nil
}

func calculateIntegers(a, b Value, operator byte) (res Value, err error) {
	var xa, xb int64

	xa, err = a.ConvertToInt64()
	if err != nil {
		return NewNullValue(), nil
	}

	xb, err = b.ConvertToInt64()
	if err != nil {
		return NewNullValue(), nil
	}

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

	xa, err = a.ConvertToFloat64()
	if err != nil {
		return NewNullValue(), nil
	}

	xb, err = b.ConvertToFloat64()
	if err != nil {
		return NewNullValue(), nil
	}

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
