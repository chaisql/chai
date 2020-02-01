package document

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
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
	documentZeroValue = NewZeroValue(DocumentValue)
)

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
	}

	return ""
}

// IsNumber returns true if t is either an integer of a float.
func (t ValueType) IsNumber() bool {
	return t.IsInteger() || t.IsFloat()
}

// IsInteger returns true if t is a signed or unsigned integer of any size.
func (t ValueType) IsInteger() bool {
	return t >= Int8Value && t <= Int64Value
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

// NewValue creates a value whose type is infered from x.
func NewValue(x interface{}) (Value, error) {
	switch v := x.(type) {
	case []byte:
		return NewBlobValue(v), nil
	case string:
		return NewTextValue(v), nil
	case bool:
		return NewBoolValue(v), nil
	case int:
		return NewIntValue(v), nil
	case int8:
		return NewIntValue(int(v)), nil
	case int16:
		return NewIntValue(int(v)), nil
	case int32:
		return NewIntValue(int(v)), nil
	case int64:
		return NewIntValue(int(v)), nil
	case uint:
		if v <= math.MaxInt64 {
			return NewIntValue(int(v)), nil
		}

		return NewFloat64Value(float64(v)), nil
	case uint8:
		return NewIntValue(int(v)), nil
	case uint16:
		return NewIntValue(int(v)), nil
	case uint32:
		return NewIntValue(int(v)), nil
	case uint64:
		if v <= math.MaxInt64 {
			return NewIntValue(int(v)), nil
		}

		return NewFloat64Value(float64(v)), nil
	case float64:
		return NewFloat64Value(v), nil
	case nil:
		return NewNullValue(), nil
	case Document:
		return NewDocumentValue(v), nil
	}

	ref := reflect.Indirect(reflect.ValueOf(x))
	switch ref.Kind() {
	case reflect.Struct:
		doc, err := NewFromStruct(x)
		if err != nil {
			return Value{}, err
		}
		return NewDocumentValue(doc), nil
	case reflect.Slice, reflect.Array:
		return NewArrayValue(&sliceArray{ref}), nil
	}

	return Value{}, fmt.Errorf("unsupported type %T", x)
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

// NewArrayValue returns a value of type Array.
func NewArrayValue(a Array) Value {
	return Value{
		Type: ArrayValue,
		V:    a,
	}
}

func intToValue(x int64) Value {
	switch {
	case x <= math.MaxInt8:
		return NewInt8Value(int8(x))
	case x <= math.MaxInt16:
		return NewInt16Value(int16(x))
	case x <= math.MaxInt32:
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
	}

	return Value{}
}

// IsTruthy returns whether v is not equal to the zero value of its type.
func (v Value) IsTruthy() bool {
	return !v.IsZeroValue()
}

// String returns a string representation of the value. It implements the fmt.Stringer interface.
func (v Value) String() string {
	switch v.Type {
	case DocumentValue:
		var buf bytes.Buffer
		err := ToJSON(&buf, v.V.(Document))
		if err != nil {
			panic(err)
		}
		return buf.String()
	case ArrayValue:
		var buf bytes.Buffer
		err := ArrayToJSON(&buf, v.V.(Array))
		if err != nil {
			panic(err)
		}
		return buf.String()
	case NullValue:
		return "NULL"
	case TextValue:
		return string(v.V.([]byte))
	}

	return fmt.Sprintf("%v", v.V)
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

// IsZeroValue indicates if the value data is the zero value for the value type.
// This function doesn't perform any allocation.
func (v Value) IsZeroValue() bool {
	switch v.Type {
	case BlobValue, TextValue:
		return bytes.Compare(v.V.([]byte), blobZeroValue.V.([]byte)) == 0
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
	case DocumentValue:
		return v.V == documentZeroValue.V
	case NullValue:
		return false
	}

	return false
}

// MarshalJSON implements the json.Marshaler interface.
func (v Value) MarshalJSON() ([]byte, error) {
	var x interface{}

	switch v.Type {
	case DocumentValue:
		d, err := v.ConvertToDocument()
		if err != nil {
			return nil, err
		}
		x = &jsonDocument{d}
	case ArrayValue:
		a, err := v.ConvertToArray()
		if err != nil {
			return nil, err
		}
		x = &jsonArray{a}
	case TextValue, BlobValue:
		s, err := v.ConvertToText()
		if err != nil {
			return nil, err
		}
		x = s
	default:
		x = v.V
	}

	return json.Marshal(x)
}

// Scan v into t.
func (v Value) Scan(t interface{}) error {
	return scanValue(v, reflect.ValueOf(t))
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
	}

	return i, nil
}
