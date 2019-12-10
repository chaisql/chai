package document

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
)

var (
	bytesZeroValue    = NewZeroValue(BytesValue)
	stringZeroValue   = NewZeroValue(StringValue)
	boolZeroValue     = NewZeroValue(BoolValue)
	uintZeroValue     = NewZeroValue(UintValue)
	uint8ZeroValue    = NewZeroValue(Uint8Value)
	uint16ZeroValue   = NewZeroValue(Uint16Value)
	uint32ZeroValue   = NewZeroValue(Uint32Value)
	uint64ZeroValue   = NewZeroValue(Uint64Value)
	intZeroValue      = NewZeroValue(IntValue)
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
	BytesValue ValueType = iota + 1
	StringValue
	BoolValue
	UintValue
	Uint8Value
	Uint16Value
	Uint32Value
	Uint64Value
	IntValue
	Int8Value
	Int16Value
	Int32Value
	Int64Value
	Float64Value

	NullValue

	DocumentValue
	ArrayValue
)

// NewValueTypeFromGoType returns the Type corresponding to the given Go type.
func NewValueTypeFromGoType(tp string) ValueType {
	switch tp {
	case "[]byte":
		return BytesValue
	case "string":
		return StringValue
	case "bool":
		return BoolValue
	case "uint":
		return UintValue
	case "uint8":
		return Uint8Value
	case "uint16":
		return Uint16Value
	case "uint32":
		return Uint32Value
	case "uint64":
		return Uint64Value
	case "int":
		return IntValue
	case "int8":
		return Int8Value
	case "int16":
		return Int16Value
	case "int32":
		return Int32Value
	case "int64":
		return Int64Value
	case "float64":
		return Float64Value
	case "nil":
		return NullValue
	case "struct":
		return DocumentValue
	}

	return 0
}

func (t ValueType) String() string {
	switch t {
	case BytesValue:
		return "Bytes"
	case StringValue:
		return "String"
	case BoolValue:
		return "Bool"
	case UintValue:
		return "Uint"
	case Uint8Value:
		return "Uint8"
	case Uint16Value:
		return "Uint16"
	case Uint32Value:
		return "Uint32"
	case Uint64Value:
		return "Uint64"
	case IntValue:
		return "Int"
	case Int8Value:
		return "Int8"
	case Int16Value:
		return "Int16"
	case Int32Value:
		return "Int32"
	case Int64Value:
		return "Int64"
	case Float64Value:
		return "Float64"
	case NullValue:
		return "Null"
	case DocumentValue:
		return "Document"
	case ArrayValue:
		return "Array"
	}

	return ""
}

// IsNumber returns true if t is either an integer of a float.
func (t ValueType) IsNumber() bool {
	return t.IsInteger() || t.IsFloat()
}

// IsInteger returns true if t is a signed or unsigned integer of any size.
func (t ValueType) IsInteger() bool {
	return t >= UintValue && t <= Int64Value
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
		return NewBytesValue(v), nil
	case string:
		return NewStringValue(v), nil
	case bool:
		return NewBoolValue(v), nil
	case uint:
		return NewUintValue(v), nil
	case uint8:
		return NewUint8Value(v), nil
	case uint16:
		return NewUint16Value(v), nil
	case uint32:
		return NewUint32Value(v), nil
	case uint64:
		return NewUint64Value(v), nil
	case int:
		return NewIntValue(v), nil
	case int8:
		return NewInt8Value(v), nil
	case int16:
		return NewInt16Value(v), nil
	case int32:
		return NewInt32Value(v), nil
	case int64:
		return NewInt64Value(v), nil
	case float64:
		return NewFloat64Value(v), nil
	case nil:
		return NewNullValue(), nil
	case Document:
		return NewDocumentValue(v), nil
	default:
		return Value{}, fmt.Errorf("unsupported type %T", x)
	}
}

// NewBytesValue encodes x and returns a value.
func NewBytesValue(x []byte) Value {
	return Value{
		Type: BytesValue,
		V:    x,
	}
}

// NewStringValue encodes x and returns a value.
func NewStringValue(x string) Value {
	return Value{
		Type: StringValue,
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

// NewUintValue encodes x and returns a value.
func NewUintValue(x uint) Value {
	return Value{
		Type: UintValue,
		V:    x,
	}
}

// NewUint8Value encodes x and returns a value.
func NewUint8Value(x uint8) Value {
	return Value{
		Type: Uint8Value,
		V:    x,
	}
}

// NewUint16Value encodes x and returns a value.
func NewUint16Value(x uint16) Value {
	return Value{
		Type: Uint16Value,
		V:    x,
	}
}

// NewUint32Value encodes x and returns a value.
func NewUint32Value(x uint32) Value {
	return Value{
		Type: Uint32Value,
		V:    x,
	}
}

// NewUint64Value encodes x and returns a value.
func NewUint64Value(x uint64) Value {
	return Value{
		Type: Uint64Value,
		V:    x,
	}
}

// NewIntValue encodes x and returns a value.
func NewIntValue(x int) Value {
	return Value{
		Type: IntValue,
		V:    x,
	}
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

// NewZeroValue returns a value whose value is equal to the Go zero value
// of the selected type.
func NewZeroValue(t ValueType) Value {
	switch t {
	case BytesValue:
		return NewBytesValue(nil)
	case StringValue:
		return NewStringValue("")
	case BoolValue:
		return NewBoolValue(false)
	case UintValue:
		return NewUintValue(0)
	case Uint8Value:
		return NewUint8Value(0)
	case Uint16Value:
		return NewUint16Value(0)
	case Uint32Value:
		return NewUint32Value(0)
	case Uint64Value:
		return NewUint64Value(0)
	case IntValue:
		return NewIntValue(0)
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
	case StringValue:
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
	case BytesValue:
		x, err := v.ConvertToBytes()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: BytesValue,
			V:    x,
		}, nil
	case StringValue:
		x, err := v.ConvertToString()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: StringValue,
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
	case UintValue:
		x, err := v.ConvertToUint()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: UintValue,
			V:    x,
		}, nil
	case Uint8Value:
		x, err := v.ConvertToUint8()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Uint8Value,
			V:    x,
		}, nil
	case Uint16Value:
		x, err := v.ConvertToUint16()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Uint16Value,
			V:    x,
		}, nil
	case Uint32Value:
		x, err := v.ConvertToUint32()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Uint32Value,
			V:    x,
		}, nil
	case Uint64Value:
		x, err := v.ConvertToUint64()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Uint64Value,
			V:    x,
		}, nil
	case IntValue:
		x, err := v.ConvertToInt()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: IntValue,
			V:    x,
		}, nil
	case Int8Value:
		x, err := v.ConvertToInt8()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Int8Value,
			V:    x,
		}, nil
	case Int16Value:
		x, err := v.ConvertToInt16()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Int16Value,
			V:    x,
		}, nil
	case Int32Value:
		x, err := v.ConvertToInt32()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Int32Value,
			V:    x,
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

// ConvertToBytes returns v.Data. It's a convenience method to ease code generation.
func (v Value) ConvertToBytes() ([]byte, error) {
	switch v.Type {
	case StringValue, BytesValue:
		return v.V.([]byte), nil
	}

	return nil, fmt.Errorf("can't convert %q to bytes", v.Type)
}

// ConvertToString turns a value of type String or Bytes into a string.
// If fails if it's used with any other type.
func (v Value) ConvertToString() (string, error) {
	switch v.Type {
	case StringValue, BytesValue:
		return string(v.V.([]byte)), nil
	}

	return "", fmt.Errorf("can't convert %q to string", v.Type)
}

// ConvertToBool returns true if v is truthy, otherwise it returns false.
func (v Value) ConvertToBool() (bool, error) {
	if v.Type == BoolValue {
		return v.V.(bool), nil
	}

	return !v.IsZeroValue(), nil
}

// ConvertToUint turns any number into a uint.
// It doesn't work with other types.
func (v Value) ConvertToUint() (uint, error) {
	if v.Type == UintValue {
		return v.V.(uint), nil
	}

	if v.Type.IsNumber() {
		x, err := convertNumberToInt64(v)
		if err != nil {
			return 0, err
		}
		return uint(x), nil
	}

	if v.Type == BoolValue {
		if v.V.(bool) {
			return 1, nil
		}

		return 0, nil
	}

	return 0, fmt.Errorf("can't convert %q to uint", v.Type)
}

// ConvertToUint8 turns any number into a uint8.
// It doesn't work with other types.
func (v Value) ConvertToUint8() (uint8, error) {
	if v.Type == Uint8Value {
		return v.V.(uint8), nil
	}

	if v.Type.IsNumber() {
		x, err := convertNumberToInt64(v)
		if err != nil {
			return 0, err
		}
		return uint8(x), nil
	}

	if v.Type == BoolValue {
		if v.V.(bool) {
			return 1, nil
		}

		return 0, nil
	}

	return 0, fmt.Errorf("can't convert %q to uint8", v.Type)
}

// ConvertToUint16 turns any number into a uint16.
// It doesn't work with other types.
func (v Value) ConvertToUint16() (uint16, error) {
	if v.Type == Uint16Value {
		return v.V.(uint16), nil
	}

	if v.Type.IsNumber() {
		x, err := convertNumberToInt64(v)
		if err != nil {
			return 0, err
		}
		return uint16(x), nil
	}

	if v.Type == BoolValue {
		if v.V.(bool) {
			return 1, nil
		}

		return 0, nil
	}

	return 0, fmt.Errorf("can't convert %q to uint16", v.Type)
}

// ConvertToUint32 turns any number into a uint32.
// It doesn't work with other types.
func (v Value) ConvertToUint32() (uint32, error) {
	if v.Type == Uint32Value {
		return v.V.(uint32), nil
	}

	if v.Type.IsNumber() {
		x, err := convertNumberToInt64(v)
		if err != nil {
			return 0, err
		}
		return uint32(x), nil
	}

	if v.Type == BoolValue {
		if v.V.(bool) {
			return 1, nil
		}

		return 0, nil
	}

	return 0, fmt.Errorf("can't convert %q to uint32", v.Type)
}

// ConvertToUint64 turns any number into a uint64.
// It doesn't work with other types.
func (v Value) ConvertToUint64() (uint64, error) {
	if v.Type == Uint64Value {
		return v.V.(uint64), nil
	}

	if v.Type.IsNumber() {
		x, err := convertNumberToInt64(v)
		if err != nil {
			return 0, err
		}
		return uint64(x), nil
	}

	if v.Type == BoolValue {
		if v.V.(bool) {
			return 1, nil
		}

		return 0, nil
	}

	return 0, fmt.Errorf("can't convert %q to uint64", v.Type)
}

// ConvertToInt turns any number into an int.
// It doesn't work with other types.
func (v Value) ConvertToInt() (int, error) {
	if v.Type == IntValue {
		return v.V.(int), nil
	}

	if v.Type.IsNumber() {
		x, err := convertNumberToInt64(v)
		if err != nil {
			return 0, err
		}
		return int(x), nil
	}

	if v.Type == BoolValue {
		if v.V.(bool) {
			return 1, nil
		}

		return 0, nil
	}

	return 0, fmt.Errorf("can't convert %q to Int", v.Type)
}

// ConvertToInt8 turns any number into an int8.
// It doesn't work with other types.
func (v Value) ConvertToInt8() (int8, error) {
	if v.Type == Int8Value {
		return v.V.(int8), nil
	}

	if v.Type.IsNumber() {
		x, err := convertNumberToInt64(v)
		if err != nil {
			return 0, err
		}
		return int8(x), nil
	}

	if v.Type == BoolValue {
		if v.V.(bool) {
			return 1, nil
		}

		return 0, nil
	}

	return 0, fmt.Errorf("can't convert %q to Int8", v.Type)
}

// ConvertToInt16 turns any number into an int16.
// It doesn't work with other types.
func (v Value) ConvertToInt16() (int16, error) {
	if v.Type == Int16Value {
		return v.V.(int16), nil
	}

	if v.Type.IsNumber() {
		x, err := convertNumberToInt64(v)
		if err != nil {
			return 0, err
		}
		return int16(x), nil
	}

	if v.Type == BoolValue {
		if v.V.(bool) {
			return 1, nil
		}

		return 0, nil
	}

	return 0, fmt.Errorf("can't convert %q to int16", v.Type)
}

// ConvertToInt32 turns any number into an int32.
// It doesn't work with other types.
func (v Value) ConvertToInt32() (int32, error) {
	if v.Type == Int32Value {
		return v.V.(int32), nil
	}

	if v.Type.IsNumber() {
		x, err := convertNumberToInt64(v)
		if err != nil {
			return 0, err
		}
		return int32(x), nil
	}

	if v.Type == BoolValue {
		if v.V.(bool) {
			return 1, nil
		}

		return 0, nil
	}

	return 0, fmt.Errorf("can't convert %q to int32", v.Type)
}

// ConvertToInt64 turns any number into an int64.
// It doesn't work with other types.
func (v Value) ConvertToInt64() (int64, error) {
	if v.Type == Int64Value {
		return v.V.(int64), nil
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
	if v.Type != DocumentValue {
		return nil, fmt.Errorf("can't convert %q to document", v.Type)
	}

	return v.V.(Document), nil
}

// ConvertToArray returns an array from the value.
// It only works if the type of v is ArrayValue.
func (v Value) ConvertToArray() (Array, error) {
	if v.Type != ArrayValue {
		return nil, fmt.Errorf("can't convert %q to array", v.Type)
	}

	return v.V.(Array), nil
}

// IsZeroValue indicates if the value data is the zero value for the value type.
// This function doesn't perform any allocation.
func (v Value) IsZeroValue() bool {
	switch v.Type {
	case BytesValue, StringValue:
		return bytes.Compare(v.V.([]byte), bytesZeroValue.V.([]byte)) == 0
	case BoolValue:
		return v.V == boolZeroValue.V
	case UintValue:
		return v.V == uintZeroValue.V
	case Uint8Value:
		return v.V == uint8ZeroValue.V
	case Uint16Value:
		return v.V == uint16ZeroValue.V
	case Uint32Value:
		return v.V == uint32ZeroValue.V
	case Uint64Value:
		return v.V == uint64ZeroValue.V
	case IntValue:
		return v.V == intZeroValue.V
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

func (v Value) MarshalJSON() ([]byte, error) {
	var x interface{}
	var err error

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
	case StringValue, BytesValue:
		s, err := v.ConvertToString()
		if err != nil {
			return nil, err
		}
		x = s
	default:
		x = v.V
	}

	if err != nil {
		return nil, err
	}

	return json.Marshal(x)
}

func convertNumberToInt64(v Value) (int64, error) {
	var i int64

	switch v.Type {
	case UintValue:
		i = int64(v.V.(uint))
	case Uint8Value:
		i = int64(v.V.(uint8))
	case Uint16Value:
		i = int64(v.V.(uint16))
	case Uint32Value:
		i = int64(v.V.(uint32))
	case Uint64Value:
		i = int64(v.V.(uint64))
	case IntValue:
		i = int64(v.V.(int))
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
		if math.Trunc(f) != f {
			return 0, errors.New("cannot convert float64 value to integer without loss of precision")
		}
		i = int64(f)
	}

	return i, nil
}
