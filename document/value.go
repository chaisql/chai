package document

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/asdine/genji/pkg/bytesutil"
)

var (
	blobZeroValue     = NewZeroValue(BlobValue)
	textZeroValue     = NewZeroValue(TextValue)
	boolZeroValue     = NewZeroValue(BoolValue)
	int8ZeroValue     = NewZeroValue(Int8Value)
	int16ZeroValue    = NewZeroValue(Int16Value)
	int32ZeroValue    = NewZeroValue(Int32Value)
	int64ZeroValue    = NewZeroValue(Int64Value)
	float64ZeroValue  = NewZeroValue(Float64Value)
	durationZeroValue = NewZeroValue(DurationValue)
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
const (
	BlobValue ValueType = iota + 1
	TextValue
	BoolValue
	Int8Value
	Int16Value
	Int32Value
	Int64Value
	Float64Value

	NullValue

	DocumentValue
	ArrayValue

	DurationValue
)

func (t ValueType) String() string {
	switch t {
	case BlobValue:
		return "blob"
	case TextValue:
		return "text"
	case BoolValue:
		return "bool"
	case Int8Value:
		return "int8"
	case Int16Value:
		return "int16"
	case Int32Value:
		return "int32"
	case Int64Value:
		return "int64"
	case Float64Value:
		return "float64"
	case NullValue:
		return "null"
	case DocumentValue:
		return "document"
	case ArrayValue:
		return "array"
	case DurationValue:
		return "duration"
	}

	return ""
}

// IsNumber returns true if t is either an integer of a float.
func (t ValueType) IsNumber() bool {
	return t.IsInteger() || t.IsFloat() || t == DurationValue
}

// IsInteger returns true if t is a signed or unsigned integer of any size.
func (t ValueType) IsInteger() bool {
	return t >= Int8Value && t <= Int64Value || t == DurationValue
}

// IsFloat returns true if t is either a Float32 or Float64.
func (t ValueType) IsFloat() bool {
	return t == Float64Value
}

// A Value stores encoded data alongside its type.
type Value struct {
	Type ValueType
	V    interface{}
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

// NewBoolValue encodes x and returns a value.
func NewBoolValue(x bool) Value {
	return Value{
		Type: BoolValue,
		V:    x,
	}
}

// NewIntValue encodes x and returns a value whose type depends on the
// magnitude of x.
func NewIntValue(x int) Value {
	return intToValue(int64(x))
}

// NewInt8Value encodes x and returns a value.
func NewInt8Value(x int8) Value {
	return Value{
		Type: Int8Value,
		V:    x,
	}
}

// NewInt16Value encodes x and returns a value.
func NewInt16Value(x int16) Value {
	return Value{
		Type: Int16Value,
		V:    x,
	}
}

// NewInt32Value encodes x and returns a value.
func NewInt32Value(x int32) Value {
	return Value{
		Type: Int32Value,
		V:    x,
	}
}

// NewInt64Value encodes x and returns a value.
func NewInt64Value(x int64) Value {
	return Value{
		Type: Int64Value,
		V:    x,
	}
}

// NewFloat64Value encodes x and returns a value.
func NewFloat64Value(x float64) Value {
	return Value{
		Type: Float64Value,
		V:    x,
	}
}

// NewNullValue returns a Null value.
func NewNullValue() Value {
	return Value{
		Type: NullValue,
	}
}

// NewDocumentValue returns a value of type Document.
func NewDocumentValue(d Document) Value {
	return Value{
		Type: DocumentValue,
		V:    d,
	}
}

// NewDurationValue returns a value of type Duration.
func NewDurationValue(d time.Duration) Value {
	return Value{
		Type: DurationValue,
		V:    d,
	}
}

// NewArrayValue returns a value of type Array.
func NewArrayValue(a Array) Value {
	return Value{
		Type: ArrayValue,
		V:    a,
	}
}

func intToValue(x int64) Value {
	switch {
	case x <= math.MaxInt8 && x >= math.MinInt8:
		return NewInt8Value(int8(x))
	case x <= math.MaxInt16 && x >= math.MinInt16:
		return NewInt16Value(int16(x))
	case x <= math.MaxInt32 && x >= math.MinInt32:
		return NewInt32Value(int32(x))
	}

	return NewInt64Value(x)
}

// NewZeroValue returns a value whose value is equal to the Go zero value
// of the selected type.
func NewZeroValue(t ValueType) Value {
	switch t {
	case BlobValue:
		return NewBlobValue(nil)
	case TextValue:
		return NewTextValue("")
	case BoolValue:
		return NewBoolValue(false)
	case Int8Value:
		return NewInt8Value(0)
	case Int16Value:
		return NewInt16Value(0)
	case Int32Value:
		return NewInt32Value(0)
	case Int64Value:
		return NewInt64Value(0)
	case Float64Value:
		return NewFloat64Value(0)
	case DocumentValue:
		return NewDocumentValue(NewFieldBuffer())
	case ArrayValue:
		return NewArrayValue(NewValueBuffer())
	case DurationValue:
		return NewDurationValue(0)
	}

	return Value{}
}

// IsTruthy returns whether v is not equal to the zero value of its type.
func (v Value) IsTruthy() bool {
	return !v.IsZeroValue()
}

// ConvertTo decodes v to the selected type when possible.
func (v Value) ConvertTo(t ValueType) (Value, error) {
	if v.Type == t {
		return v, nil
	}

	switch t {
	case BlobValue:
		x, err := v.ConvertToBlob()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: BlobValue,
			V:    x,
		}, nil
	case TextValue:
		x, err := v.ConvertToText()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: TextValue,
			V:    x,
		}, nil
	case BoolValue:
		x, err := v.ConvertToBool()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: BoolValue,
			V:    x,
		}, nil
	case Int8Value:
		x, err := v.ConvertToInt64()
		if err != nil {
			return Value{}, err
		}
		if x > math.MaxInt8 {
			return Value{}, fmt.Errorf("cannot convert %s to int8: out of range", v.Type)
		}

		return Value{
			Type: Int8Value,
			V:    int8(x),
		}, nil
	case Int16Value:
		x, err := v.ConvertToInt64()
		if err != nil {
			return Value{}, err
		}
		if x > math.MaxInt16 {
			return Value{}, fmt.Errorf("cannot convert %s to int16: out of range", v.Type)
		}
		return Value{
			Type: Int16Value,
			V:    int16(x),
		}, nil
	case Int32Value:
		x, err := v.ConvertToInt64()
		if err != nil {
			return Value{}, err
		}
		if x > math.MaxInt32 {
			return Value{}, fmt.Errorf("cannot convert %s to int32: out of range", v.Type)
		}
		return Value{
			Type: Int32Value,
			V:    int32(x),
		}, nil
	case Int64Value:
		x, err := v.ConvertToInt64()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Int64Value,
			V:    x,
		}, nil
	case Float64Value:
		x, err := v.ConvertToFloat64()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Float64Value,
			V:    x,
		}, nil
	case DurationValue:
		x, err := v.ConvertToDuration()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: DurationValue,
			V:    x,
		}, nil
	}

	return Value{}, fmt.Errorf("can't convert %q to %q", v.Type, t)
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

	return nil, fmt.Errorf("can't convert %q to bytes", v.Type)
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

	return "", fmt.Errorf("can't convert %q to string", v.Type)
}

// ConvertToBool returns true if v is truthy, otherwise it returns false.
func (v Value) ConvertToBool() (bool, error) {
	if v.Type == BoolValue {
		return v.V.(bool), nil
	}

	if v.Type == NullValue {
		return false, nil
	}

	return !v.IsZeroValue(), nil
}

// ConvertToInt64 turns any number into an int64.
// It doesn't work with other types.
func (v Value) ConvertToInt64() (int64, error) {
	if v.Type == Int64Value {
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

	return 0, fmt.Errorf("can't convert %q to int64", v.Type)
}

// ConvertToFloat64 turns any number into a float64.
// It doesn't work with other types.
func (v Value) ConvertToFloat64() (float64, error) {
	if v.Type == Float64Value {
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

	return 0, fmt.Errorf("can't convert %q to float64", v.Type)
}

// ConvertToDocument returns a document from the value.
// It only works if the type of v is DocumentValue.
func (v Value) ConvertToDocument() (Document, error) {
	if v.Type == NullValue {
		return NewFieldBuffer(), nil
	}

	if v.Type != DocumentValue {
		return nil, fmt.Errorf("can't convert %q to document", v.Type)
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
		return nil, fmt.Errorf("can't convert %q to array", v.Type)
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
			return 0, fmt.Errorf("can't convert %q to duration: %v", v.V, err)
		}
		return d, nil
	}

	x, err := v.ConvertToInt64()
	return time.Duration(x), err
}

// IsZeroValue indicates if the value data is the zero value for the value type.
// This function doesn't perform any allocation.
func (v Value) IsZeroValue() bool {
	switch v.Type {
	case BlobValue, TextValue:
		return bytesutil.CompareBytes(v.V.([]byte), blobZeroValue.V.([]byte)) == 0
	case BoolValue:
		return v.V == boolZeroValue.V
	case Int8Value:
		return v.V == int8ZeroValue.V
	case Int16Value:
		return v.V == int16ZeroValue.V
	case Int32Value:
		return v.V == int32ZeroValue.V
	case Int64Value:
		return v.V == int64ZeroValue.V
	case Float64Value:
		return v.V == float64ZeroValue.V
	case DurationValue:
		return v.V == durationZeroValue.V
	}

	return false
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
		a, err = a.ConvertTo(Int64Value)
		if err != nil {
			return
		}
	}

	if b.Type == BoolValue {
		b, err = b.ConvertTo(Int64Value)
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

	err = fmt.Errorf("cannot add value of type %s to value of type %s", a.Type, b.Type)
	return
}

func convertNumberToInt64(v Value) (int64, error) {
	var i int64

	switch v.Type {
	case Int8Value:
		i = int64(v.V.(int8))
	case Int16Value:
		i = int64(v.V.(int16))
	case Int32Value:
		i = int64(v.V.(int32))
	case Int64Value:
		return v.V.(int64), nil
	case Float64Value:
		f := v.V.(float64)
		if f > math.MaxInt64 {
			return i, errors.New("cannot convert float64 to integer without overflowing")
		}
		if math.Trunc(f) != f {
			return 0, errors.New("cannot convert float64 value to integer without loss of precision")
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
		return
	}

	xb, err = b.ConvertToInt64()
	if err != nil {
		return
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
			return NewFloat64Value(float64(xa) + float64(xb)), nil
		}
		return NewIntValue(int(xr)), nil
	case '*':
		if xa == 0 || xb == 0 {
			return NewIntValue(0), nil
		}

		xr = xa * xb
		// if there is no integer overflow
		// return an int, otherwise
		// convert to float
		if (xr < 0) == ((xa < 0) != (xb < 0)) {
			if xr/xb == xa {
				return NewIntValue(int(xr)), nil
			}
		}
		return NewFloat64Value(float64(xa) * float64(xb)), nil
	case '/':
		if xb == 0 {
			return NewNullValue(), nil
		}

		return NewIntValue(int(xa / xb)), nil
	case '%':
		if xb == 0 {
			return NewNullValue(), nil
		}

		return NewIntValue(int(xa % xb)), nil
	case '&':
		return NewIntValue(int(xa & xb)), nil
	case '|':
		return NewIntValue(int(xa | xb)), nil
	case '^':
		return NewIntValue(int(xa ^ xb)), nil
	default:
		panic(fmt.Sprintf("unknown operator %c", operator))
	}
}

func calculateFloats(a, b Value, operator byte) (res Value, err error) {
	var xa, xb float64

	xa, err = a.ConvertToFloat64()
	if err != nil {
		return
	}

	xb, err = b.ConvertToFloat64()
	if err != nil {
		return
	}

	switch operator {
	case '+':
		return NewFloat64Value(xa + xb), nil
	case '-':
		return NewFloat64Value(xa - xb), nil
	case '*':
		return NewFloat64Value(xa * xb), nil
	case '/':
		if xb == 0 {
			return NewNullValue(), nil
		}

		return NewFloat64Value(xa / xb), nil
	case '%':
		if xb == 0 {
			return NewNullValue(), nil
		}

		ia, ib := int64(xa), int64(xb)
		return NewFloat64Value(float64(ia % ib)), nil
	case '&':
		ia, ib := int64(xa), int64(xb)
		return NewIntValue(int(ia & ib)), nil
	case '|':
		ia, ib := int64(xa), int64(xb)
		return NewIntValue(int(ia | ib)), nil
	case '^':
		ia, ib := int64(xa), int64(xb)
		return NewIntValue(int(ia ^ ib)), nil
	default:
		panic(fmt.Sprintf("unknown operator %c", operator))
	}
}
