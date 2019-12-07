package document

import (
	"bytes"
	"encoding/binary"
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
	Data []byte
	v    interface{}
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
		Data: x,
	}
}

// NewStringValue encodes x and returns a value.
func NewStringValue(x string) Value {
	return Value{
		Type: StringValue,
		Data: []byte(x),
	}
}

// NewBoolValue encodes x and returns a value.
func NewBoolValue(x bool) Value {
	return Value{
		Type: BoolValue,
		Data: EncodeBool(x),
	}
}

// NewUintValue encodes x and returns a value.
func NewUintValue(x uint) Value {
	return Value{
		Type: UintValue,
		Data: EncodeUint(x),
	}
}

// NewUint8Value encodes x and returns a value.
func NewUint8Value(x uint8) Value {
	return Value{
		Type: Uint8Value,
		Data: EncodeUint8(x),
	}
}

// NewUint16Value encodes x and returns a value.
func NewUint16Value(x uint16) Value {
	return Value{
		Type: Uint16Value,
		Data: EncodeUint16(x),
	}
}

// NewUint32Value encodes x and returns a value.
func NewUint32Value(x uint32) Value {
	return Value{
		Type: Uint32Value,
		Data: EncodeUint32(x),
	}
}

// NewUint64Value encodes x and returns a value.
func NewUint64Value(x uint64) Value {
	return Value{
		Type: Uint64Value,
		Data: EncodeUint64(x),
	}
}

// NewIntValue encodes x and returns a value.
func NewIntValue(x int) Value {
	return Value{
		Type: IntValue,
		Data: EncodeInt(x),
	}
}

// NewInt8Value encodes x and returns a value.
func NewInt8Value(x int8) Value {
	return Value{
		Type: Int8Value,
		Data: EncodeInt8(x),
	}
}

// NewInt16Value encodes x and returns a value.
func NewInt16Value(x int16) Value {
	return Value{
		Type: Int16Value,
		Data: EncodeInt16(x),
	}
}

// NewInt32Value encodes x and returns a value.
func NewInt32Value(x int32) Value {
	return Value{
		Type: Int32Value,
		Data: EncodeInt32(x),
	}
}

// NewInt64Value encodes x and returns a value.
func NewInt64Value(x int64) Value {
	return Value{
		Type: Int64Value,
		Data: EncodeInt64(x),
	}
}

// NewFloat64Value encodes x and returns a value.
func NewFloat64Value(x float64) Value {
	return Value{
		Type: Float64Value,
		Data: EncodeFloat64(x),
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
	data, err := Encode(d)
	if err != nil {
		panic(err)
	}

	return Value{
		Type: DocumentValue,
		v:    d,
		Data: data,
	}
}

// NewArrayValue returns a value of type Array.
func NewArrayValue(a Array) Value {
	data, err := EncodeArray(a)
	if err != nil {
		panic(err)
	}

	return Value{
		Type: ArrayValue,
		v:    a,
		Data: data,
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

func (v *Value) decode() error {
	var err error

	switch v.Type {
	case BytesValue:
		v.v, err = DecodeBytes(v.Data)
	case StringValue:
		v.v, err = DecodeString(v.Data)
	case BoolValue:
		v.v, err = DecodeBool(v.Data)
	case UintValue:
		v.v, err = DecodeUint(v.Data)
	case Uint8Value:
		v.v, err = DecodeUint8(v.Data)
	case Uint16Value:
		v.v, err = DecodeUint16(v.Data)
	case Uint32Value:
		v.v, err = DecodeUint32(v.Data)
	case Uint64Value:
		v.v, err = DecodeUint64(v.Data)
	case IntValue:
		v.v, err = DecodeInt(v.Data)
	case Int8Value:
		v.v, err = DecodeInt8(v.Data)
	case Int16Value:
		v.v, err = DecodeInt16(v.Data)
	case Int32Value:
		v.v, err = DecodeInt32(v.Data)
	case Int64Value:
		v.v, err = DecodeInt64(v.Data)
	case Float64Value:
		v.v, err = DecodeFloat64(v.Data)
	case NullValue:
		v.v = nil
	default:
		return errors.New("unknown type")
	}

	return err
}

// Decode a value based on its type, caches it and returns its Go value.
// If the decoded value is already cached, returns it immediatly.
func (v Value) Decode() (interface{}, error) {
	if v.v == nil {
		err := v.decode()
		if err != nil {
			return nil, err
		}
	}

	return v.v, nil
}

// IsTruthy returns whether v is not equal to the zero value of its type.
func (v Value) IsTruthy() bool {
	return !v.IsZeroValue()
}

// String returns a string representation of the value. It implements the fmt.Stringer interface.
func (v Value) String() string {
	var vv interface{}

	switch v.Type {
	case BytesValue:
		vv, _ = v.DecodeToBytes()
	case StringValue:
		vv, _ = v.DecodeToString()
	case BoolValue:
		vv, _ = v.DecodeToBool()
	case UintValue:
		vv, _ = v.DecodeToUint()
	case Uint8Value:
		vv, _ = v.DecodeToUint8()
	case Uint16Value:
		vv, _ = v.DecodeToUint16()
	case Uint32Value:
		vv, _ = v.DecodeToUint32()
	case Uint64Value:
		vv, _ = v.DecodeToUint64()
	case IntValue:
		vv, _ = v.DecodeToInt()
	case Int8Value:
		vv, _ = v.DecodeToInt8()
	case Int16Value:
		vv, _ = v.DecodeToInt16()
	case Int32Value:
		vv, _ = v.DecodeToInt32()
	case Int64Value:
		vv, _ = v.DecodeToInt64()
	case Float64Value:
		vv, _ = v.DecodeToFloat64()
	case DocumentValue:
		d, _ := v.DecodeToDocument()
		var buf bytes.Buffer
		err := ToJSON(&buf, d)
		if err != nil {
			panic(err)
		}
		return buf.String()
	case ArrayValue:
		a, _ := v.DecodeToArray()
		var buf bytes.Buffer
		err := ArrayToJSON(&buf, a)
		if err != nil {
			panic(err)
		}
		return buf.String()
	case NullValue:
		return "NULL"
	}

	return fmt.Sprintf("%v", vv)
}

// ConvertTo decodes v to the selected type when possible.
func (v Value) ConvertTo(t ValueType) (Value, error) {
	if v.Type == t {
		return v, nil
	}

	switch t {
	case BytesValue:
		x, err := v.DecodeToBytes()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: BytesValue,
			Data: EncodeBytes(x),
			v:    x,
		}, nil
	case StringValue:
		x, err := v.DecodeToString()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: StringValue,
			Data: EncodeString(x),
			v:    x,
		}, nil
	case BoolValue:
		x, err := v.DecodeToBool()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: BoolValue,
			Data: EncodeBool(x),
			v:    x,
		}, nil
	case UintValue:
		x, err := v.DecodeToUint()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: UintValue,
			Data: EncodeUint(x),
			v:    x,
		}, nil
	case Uint8Value:
		x, err := v.DecodeToUint8()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Uint8Value,
			Data: EncodeUint8(x),
			v:    x,
		}, nil
	case Uint16Value:
		x, err := v.DecodeToUint16()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Uint16Value,
			Data: EncodeUint16(x),
			v:    x,
		}, nil
	case Uint32Value:
		x, err := v.DecodeToUint32()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Uint32Value,
			Data: EncodeUint32(x),
			v:    x,
		}, nil
	case Uint64Value:
		x, err := v.DecodeToUint64()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Uint64Value,
			Data: EncodeUint64(x),
			v:    x,
		}, nil
	case IntValue:
		x, err := v.DecodeToInt()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: IntValue,
			Data: EncodeInt(x),
			v:    x,
		}, nil
	case Int8Value:
		x, err := v.DecodeToInt8()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Int8Value,
			Data: EncodeInt8(x),
			v:    x,
		}, nil
	case Int16Value:
		x, err := v.DecodeToInt16()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Int16Value,
			Data: EncodeInt16(x),
			v:    x,
		}, nil
	case Int32Value:
		x, err := v.DecodeToInt32()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Int32Value,
			Data: EncodeInt32(x),
			v:    x,
		}, nil
	case Int64Value:
		x, err := v.DecodeToInt64()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Int64Value,
			Data: EncodeInt64(x),
			v:    x,
		}, nil
	case Float64Value:
		x, err := v.DecodeToFloat64()
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: Float64Value,
			Data: EncodeFloat64(x),
			v:    x,
		}, nil
	}

	return Value{}, fmt.Errorf("can't convert %q to %q", v.Type, t)
}

// DecodeToBytes returns v.Data. It's a convenience method to ease code generation.
func (v Value) DecodeToBytes() ([]byte, error) {
	return v.Data, nil
}

// DecodeToString turns a value of type String or Bytes into a string.
// If fails if it's used with any other type.
func (v Value) DecodeToString() (string, error) {
	if v.Type == StringValue {
		return DecodeString(v.Data)
	}

	if v.Type == BytesValue {
		return string(v.Data), nil
	}

	return "", fmt.Errorf("can't convert %q to string", v.Type)
}

// DecodeToBool returns true if v is truthy, otherwise it returns false.
func (v Value) DecodeToBool() (bool, error) {
	if v.Type == BoolValue {
		return DecodeBool(v.Data)
	}

	return !v.IsZeroValue(), nil
}

// DecodeToUint turns any number into a uint.
// It doesn't work with other types.
func (v Value) DecodeToUint() (uint, error) {
	if v.Type == UintValue {
		return DecodeUint(v.Data)
	}

	if v.Type.IsNumber() {
		x, err := decodeAsInt64(v)
		if err != nil {
			return 0, err
		}
		return uint(x), nil
	}

	return 0, fmt.Errorf("can't convert %q to uint", v.Type)
}

// DecodeToUint8 turns any number into a uint8.
// It doesn't work with other types.
func (v Value) DecodeToUint8() (uint8, error) {
	if v.Type == Uint8Value {
		return DecodeUint8(v.Data)
	}

	if v.Type.IsNumber() {
		x, err := decodeAsInt64(v)
		if err != nil {
			return 0, err
		}
		return uint8(x), nil
	}

	return 0, fmt.Errorf("can't convert %q to uint8", v.Type)
}

// DecodeToUint16 turns any number into a uint16.
// It doesn't work with other types.
func (v Value) DecodeToUint16() (uint16, error) {
	if v.Type == Uint16Value {
		return DecodeUint16(v.Data)
	}

	if v.Type.IsNumber() {
		x, err := decodeAsInt64(v)
		if err != nil {
			return 0, err
		}
		return uint16(x), nil
	}

	return 0, fmt.Errorf("can't convert %q to uint16", v.Type)
}

// DecodeToUint32 turns any number into a uint32.
// It doesn't work with other types.
func (v Value) DecodeToUint32() (uint32, error) {
	if v.Type == Uint32Value {
		return DecodeUint32(v.Data)
	}

	if v.Type.IsNumber() {
		x, err := decodeAsInt64(v)
		if err != nil {
			return 0, err
		}
		return uint32(x), nil
	}

	return 0, fmt.Errorf("can't convert %q to uint32", v.Type)
}

// DecodeToUint64 turns any number into a uint64.
// It doesn't work with other types.
func (v Value) DecodeToUint64() (uint64, error) {
	if v.Type == Uint64Value {
		return DecodeUint64(v.Data)
	}

	if v.Type.IsNumber() {
		x, err := decodeAsInt64(v)
		if err != nil {
			return 0, err
		}
		return uint64(x), nil
	}

	return 0, fmt.Errorf("can't convert %q to uint64", v.Type)
}

// DecodeToInt turns any number into an int.
// It doesn't work with other types.
func (v Value) DecodeToInt() (int, error) {
	if v.Type == IntValue {
		return DecodeInt(v.Data)
	}

	if v.Type.IsNumber() {
		x, err := decodeAsInt64(v)
		if err != nil {
			return 0, err
		}
		return int(x), nil
	}

	return 0, fmt.Errorf("can't convert %q to Int", v.Type)
}

// DecodeToInt8 turns any number into an int8.
// It doesn't work with other types.
func (v Value) DecodeToInt8() (int8, error) {
	if v.Type == Int8Value {
		return DecodeInt8(v.Data)
	}

	if v.Type.IsNumber() {
		x, err := decodeAsInt64(v)
		if err != nil {
			return 0, err
		}
		return int8(x), nil
	}

	return 0, fmt.Errorf("can't convert %q to Int8", v.Type)
}

// DecodeToInt16 turns any number into an int16.
// It doesn't work with other types.
func (v Value) DecodeToInt16() (int16, error) {
	if v.Type == Int16Value {
		return DecodeInt16(v.Data)
	}

	if v.Type.IsNumber() {
		x, err := decodeAsInt64(v)
		if err != nil {
			return 0, err
		}
		return int16(x), nil
	}

	return 0, fmt.Errorf("can't convert %q to int16", v.Type)
}

// DecodeToInt32 turns any number into an int32.
// It doesn't work with other types.
func (v Value) DecodeToInt32() (int32, error) {
	if v.Type == Int32Value {
		return DecodeInt32(v.Data)
	}

	if v.Type.IsNumber() {
		x, err := decodeAsInt64(v)
		if err != nil {
			return 0, err
		}
		return int32(x), nil
	}

	return 0, fmt.Errorf("can't convert %q to int32", v.Type)
}

// DecodeToInt64 turns any number into an int64.
// It doesn't work with other types.
func (v Value) DecodeToInt64() (int64, error) {
	if v.Type == Int64Value {
		return DecodeInt64(v.Data)
	}

	if v.Type.IsNumber() {
		return decodeAsInt64(v)
	}

	return 0, fmt.Errorf("can't convert %q to int64", v.Type)
}

// DecodeToFloat64 turns any number into a float64.
// It doesn't work with other types.
func (v Value) DecodeToFloat64() (float64, error) {
	if v.Type == Float64Value {
		return DecodeFloat64(v.Data)
	}

	if v.Type.IsInteger() {
		x, err := decodeAsInt64(v)
		if err != nil {
			return 0, err
		}
		return float64(x), nil
	}

	return 0, fmt.Errorf("can't convert %q to float64", v.Type)
}

// DecodeToDocument returns a document from the value.
// It only works if the type of v is DocumentValue.
func (v Value) DecodeToDocument() (Document, error) {
	if v.Type != DocumentValue {
		return nil, fmt.Errorf("can't convert %q to document", v.Type)
	}

	if v.v != nil {
		return v.v.(Document), nil
	}

	return EncodedDocument(v.Data), nil
}

// DecodeToArray returns an array from the value.
// It only works if the type of v is ArrayValue.
func (v Value) DecodeToArray() (Array, error) {
	if v.Type != ArrayValue {
		return nil, fmt.Errorf("can't convert %q to array", v.Type)
	}

	if v.v != nil {
		return v.v.(Array), nil
	}

	return EncodedArray(v.Data), nil
}

// IsZeroValue indicates if the value data is the zero value for the value type.
// This function doesn't perform any allocation.
func (v Value) IsZeroValue() bool {
	switch v.Type {
	case BytesValue:
		return bytes.Equal(v.Data, bytesZeroValue.Data)
	case StringValue:
		return bytes.Equal(v.Data, stringZeroValue.Data)
	case BoolValue:
		return bytes.Equal(v.Data, boolZeroValue.Data)
	case UintValue:
		return bytes.Equal(v.Data, uintZeroValue.Data)
	case Uint8Value:
		return bytes.Equal(v.Data, uint8ZeroValue.Data)
	case Uint16Value:
		return bytes.Equal(v.Data, uint16ZeroValue.Data)
	case Uint32Value:
		return bytes.Equal(v.Data, uint32ZeroValue.Data)
	case Uint64Value:
		return bytes.Equal(v.Data, uint64ZeroValue.Data)
	case IntValue:
		return bytes.Equal(v.Data, intZeroValue.Data)
	case Int8Value:
		return bytes.Equal(v.Data, int8ZeroValue.Data)
	case Int16Value:
		return bytes.Equal(v.Data, int16ZeroValue.Data)
	case Int32Value:
		return bytes.Equal(v.Data, int32ZeroValue.Data)
	case Int64Value:
		return bytes.Equal(v.Data, int64ZeroValue.Data)
	case Float64Value:
		return bytes.Equal(v.Data, float64ZeroValue.Data)
	case DocumentValue:
		return bytes.Equal(v.Data, documentZeroValue.Data)
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
		d, err := v.DecodeToDocument()
		if err != nil {
			return nil, err
		}
		x = &jsonDocument{d}
	case ArrayValue:
		a, err := v.DecodeToArray()
		if err != nil {
			return nil, err
		}
		x = &jsonArray{a}
	default:
		x, err = v.Decode()
	}

	if err != nil {
		return nil, err
	}

	return json.Marshal(x)
}

func decodeAsInt64(v Value) (int64, error) {
	var i int64

	switch v.Type {
	case UintValue:
		x, err := DecodeUint(v.Data)
		if err != nil {
			return 0, err
		}
		i = int64(x)
	case Uint8Value:
		x, err := DecodeUint8(v.Data)
		if err != nil {
			return 0, err
		}
		i = int64(x)
	case Uint16Value:
		x, err := DecodeUint16(v.Data)
		if err != nil {
			return 0, err
		}
		i = int64(x)
	case Uint32Value:
		x, err := DecodeUint32(v.Data)
		if err != nil {
			return 0, err
		}
		i = int64(x)
	case Uint64Value:
		x, err := DecodeUint64(v.Data)
		if err != nil {
			return 0, err
		}
		i = int64(x)
	case IntValue:
		x, err := DecodeInt(v.Data)
		if err != nil {
			return 0, err
		}
		i = int64(x)
	case Int8Value:
		x, err := DecodeInt8(v.Data)
		if err != nil {
			return 0, err
		}
		i = int64(x)
	case Int16Value:
		x, err := DecodeInt16(v.Data)
		if err != nil {
			return 0, err
		}
		i = int64(x)
	case Int32Value:
		x, err := DecodeInt32(v.Data)
		if err != nil {
			return 0, err
		}
		i = int64(x)
	case Int64Value:
		return DecodeInt64(v.Data)
	case Float64Value:
		x, err := DecodeFloat64(v.Data)
		if err != nil {
			return 0, err
		}
		if math.Trunc(x) != x {
			return 0, errors.New("cannot convert float64 value to integer without loss of precision")
		}
		i = int64(x)
	}

	return i, nil
}

// EncodeBytes takes a bytes and returns it.
// It is present to ease code generation.
func EncodeBytes(x []byte) []byte {
	return x
}

// DecodeBytes takes a byte slice and returns it.
// It is present to ease code generation.
func DecodeBytes(buf []byte) ([]byte, error) {
	return buf, nil
}

// EncodeString takes a string and returns its binary representation.
func EncodeString(x string) []byte {
	return []byte(x)
}

// DecodeString takes a byte slice and decodes it into a string.
func DecodeString(buf []byte) (string, error) {
	return string(buf), nil
}

// EncodeBool takes a bool and returns its binary representation.
func EncodeBool(x bool) []byte {
	if x {
		return []byte{1}
	}
	return []byte{0}
}

// DecodeBool takes a byte slice and decodes it into a boolean.
func DecodeBool(buf []byte) (bool, error) {
	if len(buf) != 1 {
		return false, errors.New("cannot decode buffer to bool")
	}
	return buf[0] == 1, nil
}

// EncodeUint takes an uint and returns its binary representation.
func EncodeUint(x uint) []byte {
	return EncodeUint64(uint64(x))
}

// DecodeUint takes a byte slice and decodes it into a uint.
func DecodeUint(buf []byte) (uint, error) {
	x, err := DecodeUint64(buf)
	return uint(x), err
}

// EncodeUint8 takes an uint8 and returns its binary representation.
func EncodeUint8(x uint8) []byte {
	return []byte{x}
}

// DecodeUint8 takes a byte slice and decodes it into a uint8.
func DecodeUint8(buf []byte) (uint8, error) {
	if len(buf) == 0 {
		return 0, errors.New("cannot decode buffer to uint8")
	}

	return buf[0], nil
}

// EncodeUint16 takes an uint16 and returns its binary representation.
func EncodeUint16(x uint16) []byte {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], x)
	return buf[:]
}

// DecodeUint16 takes a byte slice and decodes it into a uint16.
func DecodeUint16(buf []byte) (uint16, error) {
	if len(buf) < 2 {
		return 0, errors.New("cannot decode buffer to uint16")
	}

	return binary.BigEndian.Uint16(buf), nil
}

// EncodeUint32 takes an uint32 and returns its binary representation.
func EncodeUint32(x uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], x)
	return buf[:]
}

// DecodeUint32 takes a byte slice and decodes it into a uint32.
func DecodeUint32(buf []byte) (uint32, error) {
	if len(buf) < 4 {
		return 0, errors.New("cannot decode buffer to uint32")
	}

	return binary.BigEndian.Uint32(buf), nil
}

// EncodeUint64 takes an uint64 and returns its binary representation.
func EncodeUint64(x uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], x)
	return buf[:]
}

// DecodeUint64 takes a byte slice and decodes it into a uint64.
func DecodeUint64(buf []byte) (uint64, error) {
	if len(buf) < 8 {
		return 0, errors.New("cannot decode buffer to uint64")
	}

	return binary.BigEndian.Uint64(buf), nil
}

// EncodeInt takes an int and returns its binary representation.
func EncodeInt(x int) []byte {
	return EncodeInt64(int64(x))
}

// DecodeInt takes a byte slice and decodes it into an int.
func DecodeInt(buf []byte) (int, error) {
	x, err := DecodeInt64(buf)
	return int(x), err
}

// EncodeInt8 takes an int8 and returns its binary representation.
func EncodeInt8(x int8) []byte {
	return []byte{uint8(x + math.MaxInt8 + 1)}
}

// DecodeInt8 takes a byte slice and decodes it into an int8.
func DecodeInt8(buf []byte) (int8, error) {
	return int8(buf[0] - math.MaxInt8 - 1), nil
}

// EncodeInt16 takes an int16 and returns its binary representation.
func EncodeInt16(x int16) []byte {
	var buf [2]byte

	binary.BigEndian.PutUint16(buf[:], uint16(x)+math.MaxInt16+1)
	return buf[:]
}

// DecodeInt16 takes a byte slice and decodes it into an int16.
func DecodeInt16(buf []byte) (int16, error) {
	x, err := DecodeUint16(buf)
	x -= math.MaxInt16 + 1
	return int16(x), err
}

// EncodeInt32 takes an int32 and returns its binary representation.
func EncodeInt32(x int32) []byte {
	var buf [4]byte

	binary.BigEndian.PutUint32(buf[:], uint32(x)+math.MaxInt32+1)
	return buf[:]
}

// DecodeInt32 takes a byte slice and decodes it into an int32.
func DecodeInt32(buf []byte) (int32, error) {
	x, err := DecodeUint32(buf)
	x -= math.MaxInt32 + 1
	return int32(x), err
}

// EncodeInt64 takes an int64 and returns its binary representation.
func EncodeInt64(x int64) []byte {
	var buf [8]byte

	binary.BigEndian.PutUint64(buf[:], uint64(x)+math.MaxInt64+1)
	return buf[:]
}

// DecodeInt64 takes a byte slice and decodes it into an int64.
func DecodeInt64(buf []byte) (int64, error) {
	x, err := DecodeUint64(buf)
	x -= math.MaxInt64 + 1
	return int64(x), err
}

// EncodeFloat64 takes an float64 and returns its binary representation.
func EncodeFloat64(x float64) []byte {
	fb := math.Float64bits(x)
	if x >= 0 {
		fb ^= 1 << 63
	} else {
		fb ^= 1<<64 - 1
	}
	return EncodeUint64(fb)
}

// DecodeFloat64 takes a byte slice and decodes it into an float64.
func DecodeFloat64(buf []byte) (float64, error) {
	x := binary.BigEndian.Uint64(buf)

	if (x & (1 << 63)) != 0 {
		x ^= 1 << 63
	} else {
		x ^= 1<<64 - 1
	}
	return math.Float64frombits(x), nil
}
