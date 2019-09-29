package q

import (
	"github.com/asdine/genji/query/expr"
	"github.com/asdine/genji/value"
)

// BytesFieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type BytesFieldSelector struct {
	Field
}

// BytesField creates a typed FieldSelector for fields of type []byte.
func BytesField(name string) BytesFieldSelector {
	return BytesFieldSelector{Field: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f BytesFieldSelector) Eq(x []byte) expr.Expr {
	return expr.Eq(f.Field, expr.BytesValue(x))
}

// Gt matches if x is greater than the field selected by f.
func (f BytesFieldSelector) Gt(x []byte) expr.Expr {
	return expr.Gt(f.Field, expr.BytesValue(x))
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f BytesFieldSelector) Gte(x []byte) expr.Expr {
	return expr.Gte(f.Field, expr.BytesValue(x))
}

// Lt matches if x is less than the field selected by f.
func (f BytesFieldSelector) Lt(x []byte) expr.Expr {
	return expr.Lt(f.Field, expr.BytesValue(x))
}

// Lte matches if x is less than or equal to the field selected by f.
func (f BytesFieldSelector) Lte(x []byte) expr.Expr {
	return expr.Lte(f.Field, expr.BytesValue(x))
}

// Value returns a scalar that can be used as an expression.
func (f BytesFieldSelector) Value(x []byte) *value.Value {
	return &value.Value{
		Type: value.Bytes,
		Data: value.EncodeBytes(x),
	}
}

// StringFieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type StringFieldSelector struct {
	Field
}

// StringField creates a typed FieldSelector for fields of type string.
func StringField(name string) StringFieldSelector {
	return StringFieldSelector{Field: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f StringFieldSelector) Eq(x string) expr.Expr {
	return expr.Eq(f.Field, expr.StringValue(x))
}

// Gt matches if x is greater than the field selected by f.
func (f StringFieldSelector) Gt(x string) expr.Expr {
	return expr.Gt(f.Field, expr.StringValue(x))
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f StringFieldSelector) Gte(x string) expr.Expr {
	return expr.Gte(f.Field, expr.StringValue(x))
}

// Lt matches if x is less than the field selected by f.
func (f StringFieldSelector) Lt(x string) expr.Expr {
	return expr.Lt(f.Field, expr.StringValue(x))
}

// Lte matches if x is less than or equal to the field selected by f.
func (f StringFieldSelector) Lte(x string) expr.Expr {
	return expr.Lte(f.Field, expr.StringValue(x))
}

// Value returns a scalar that can be used as an expression.
func (f StringFieldSelector) Value(x string) *value.Value {
	return &value.Value{
		Type: value.String,
		Data: value.EncodeString(x),
	}
}

// BoolFieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type BoolFieldSelector struct {
	Field
}

// BoolField creates a typed FieldSelector for fields of type bool.
func BoolField(name string) BoolFieldSelector {
	return BoolFieldSelector{Field: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f BoolFieldSelector) Eq(x bool) expr.Expr {
	return expr.Eq(f.Field, expr.BoolValue(x))
}

// Gt matches if x is greater than the field selected by f.
func (f BoolFieldSelector) Gt(x bool) expr.Expr {
	return expr.Gt(f.Field, expr.BoolValue(x))
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f BoolFieldSelector) Gte(x bool) expr.Expr {
	return expr.Gte(f.Field, expr.BoolValue(x))
}

// Lt matches if x is less than the field selected by f.
func (f BoolFieldSelector) Lt(x bool) expr.Expr {
	return expr.Lt(f.Field, expr.BoolValue(x))
}

// Lte matches if x is less than or equal to the field selected by f.
func (f BoolFieldSelector) Lte(x bool) expr.Expr {
	return expr.Lte(f.Field, expr.BoolValue(x))
}

// Value returns a scalar that can be used as an expression.
func (f BoolFieldSelector) Value(x bool) *value.Value {
	return &value.Value{
		Type: value.Bool,
		Data: value.EncodeBool(x),
	}
}

// UintFieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type UintFieldSelector struct {
	Field
}

// UintField creates a typed FieldSelector for fields of type uint.
func UintField(name string) UintFieldSelector {
	return UintFieldSelector{Field: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f UintFieldSelector) Eq(x uint) expr.Expr {
	return expr.Eq(f.Field, expr.UintValue(x))
}

// Gt matches if x is greater than the field selected by f.
func (f UintFieldSelector) Gt(x uint) expr.Expr {
	return expr.Gt(f.Field, expr.UintValue(x))
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f UintFieldSelector) Gte(x uint) expr.Expr {
	return expr.Gte(f.Field, expr.UintValue(x))
}

// Lt matches if x is less than the field selected by f.
func (f UintFieldSelector) Lt(x uint) expr.Expr {
	return expr.Lt(f.Field, expr.UintValue(x))
}

// Lte matches if x is less than or equal to the field selected by f.
func (f UintFieldSelector) Lte(x uint) expr.Expr {
	return expr.Lte(f.Field, expr.UintValue(x))
}

// Value returns a scalar that can be used as an expression.
func (f UintFieldSelector) Value(x uint) *value.Value {
	return &value.Value{
		Type: value.Uint,
		Data: value.EncodeUint(x),
	}
}

// Uint8FieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type Uint8FieldSelector struct {
	Field
}

// Uint8Field creates a typed FieldSelector for fields of type uint8.
func Uint8Field(name string) Uint8FieldSelector {
	return Uint8FieldSelector{Field: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Uint8FieldSelector) Eq(x uint8) expr.Expr {
	return expr.Eq(f.Field, expr.Uint8Value(x))
}

// Gt matches if x is greater than the field selected by f.
func (f Uint8FieldSelector) Gt(x uint8) expr.Expr {
	return expr.Gt(f.Field, expr.Uint8Value(x))
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Uint8FieldSelector) Gte(x uint8) expr.Expr {
	return expr.Gte(f.Field, expr.Uint8Value(x))
}

// Lt matches if x is less than the field selected by f.
func (f Uint8FieldSelector) Lt(x uint8) expr.Expr {
	return expr.Lt(f.Field, expr.Uint8Value(x))
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Uint8FieldSelector) Lte(x uint8) expr.Expr {
	return expr.Lte(f.Field, expr.Uint8Value(x))
}

// Value returns a scalar that can be used as an expression.
func (f Uint8FieldSelector) Value(x uint8) *value.Value {
	return &value.Value{
		Type: value.Uint8,
		Data: value.EncodeUint8(x),
	}
}

// Uint16FieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type Uint16FieldSelector struct {
	Field
}

// Uint16Field creates a typed FieldSelector for fields of type uint16.
func Uint16Field(name string) Uint16FieldSelector {
	return Uint16FieldSelector{Field: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Uint16FieldSelector) Eq(x uint16) expr.Expr {
	return expr.Eq(f.Field, expr.Uint16Value(x))
}

// Gt matches if x is greater than the field selected by f.
func (f Uint16FieldSelector) Gt(x uint16) expr.Expr {
	return expr.Gt(f.Field, expr.Uint16Value(x))
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Uint16FieldSelector) Gte(x uint16) expr.Expr {
	return expr.Gte(f.Field, expr.Uint16Value(x))
}

// Lt matches if x is less than the field selected by f.
func (f Uint16FieldSelector) Lt(x uint16) expr.Expr {
	return expr.Lt(f.Field, expr.Uint16Value(x))
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Uint16FieldSelector) Lte(x uint16) expr.Expr {
	return expr.Lte(f.Field, expr.Uint16Value(x))
}

// Value returns a scalar that can be used as an expression.
func (f Uint16FieldSelector) Value(x uint16) *value.Value {
	return &value.Value{
		Type: value.Uint16,
		Data: value.EncodeUint16(x),
	}
}

// Uint32FieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type Uint32FieldSelector struct {
	Field
}

// Uint32Field creates a typed FieldSelector for fields of type uint32.
func Uint32Field(name string) Uint32FieldSelector {
	return Uint32FieldSelector{Field: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Uint32FieldSelector) Eq(x uint32) expr.Expr {
	return expr.Eq(f.Field, expr.Uint32Value(x))
}

// Gt matches if x is greater than the field selected by f.
func (f Uint32FieldSelector) Gt(x uint32) expr.Expr {
	return expr.Gt(f.Field, expr.Uint32Value(x))
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Uint32FieldSelector) Gte(x uint32) expr.Expr {
	return expr.Gte(f.Field, expr.Uint32Value(x))
}

// Lt matches if x is less than the field selected by f.
func (f Uint32FieldSelector) Lt(x uint32) expr.Expr {
	return expr.Lt(f.Field, expr.Uint32Value(x))
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Uint32FieldSelector) Lte(x uint32) expr.Expr {
	return expr.Lte(f.Field, expr.Uint32Value(x))
}

// Value returns a scalar that can be used as an expression.
func (f Uint32FieldSelector) Value(x uint32) *value.Value {
	return &value.Value{
		Type: value.Uint32,
		Data: value.EncodeUint32(x),
	}
}

// Uint64FieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type Uint64FieldSelector struct {
	Field
}

// Uint64Field creates a typed FieldSelector for fields of type uint64.
func Uint64Field(name string) Uint64FieldSelector {
	return Uint64FieldSelector{Field: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Uint64FieldSelector) Eq(x uint64) expr.Expr {
	return expr.Eq(f.Field, expr.Uint64Value(x))
}

// Gt matches if x is greater than the field selected by f.
func (f Uint64FieldSelector) Gt(x uint64) expr.Expr {
	return expr.Gt(f.Field, expr.Uint64Value(x))
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Uint64FieldSelector) Gte(x uint64) expr.Expr {
	return expr.Gte(f.Field, expr.Uint64Value(x))
}

// Lt matches if x is less than the field selected by f.
func (f Uint64FieldSelector) Lt(x uint64) expr.Expr {
	return expr.Lt(f.Field, expr.Uint64Value(x))
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Uint64FieldSelector) Lte(x uint64) expr.Expr {
	return expr.Lte(f.Field, expr.Uint64Value(x))
}

// Value returns a scalar that can be used as an expression.
func (f Uint64FieldSelector) Value(x uint64) *value.Value {
	return &value.Value{
		Type: value.Uint64,
		Data: value.EncodeUint64(x),
	}
}

// IntFieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type IntFieldSelector struct {
	Field
}

// IntField creates a typed FieldSelector for fields of type int.
func IntField(name string) IntFieldSelector {
	return IntFieldSelector{Field: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f IntFieldSelector) Eq(x int) expr.Expr {
	return expr.Eq(f.Field, expr.IntValue(x))
}

// Gt matches if x is greater than the field selected by f.
func (f IntFieldSelector) Gt(x int) expr.Expr {
	return expr.Gt(f.Field, expr.IntValue(x))
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f IntFieldSelector) Gte(x int) expr.Expr {
	return expr.Gte(f.Field, expr.IntValue(x))
}

// Lt matches if x is less than the field selected by f.
func (f IntFieldSelector) Lt(x int) expr.Expr {
	return expr.Lt(f.Field, expr.IntValue(x))
}

// Lte matches if x is less than or equal to the field selected by f.
func (f IntFieldSelector) Lte(x int) expr.Expr {
	return expr.Lte(f.Field, expr.IntValue(x))
}

// Value returns a scalar that can be used as an expression.
func (f IntFieldSelector) Value(x int) *value.Value {
	return &value.Value{
		Type: value.Int,
		Data: value.EncodeInt(x),
	}
}

// Int8FieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type Int8FieldSelector struct {
	Field
}

// Int8Field creates a typed FieldSelector for fields of type int8.
func Int8Field(name string) Int8FieldSelector {
	return Int8FieldSelector{Field: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Int8FieldSelector) Eq(x int8) expr.Expr {
	return expr.Eq(f.Field, expr.Int8Value(x))
}

// Gt matches if x is greater than the field selected by f.
func (f Int8FieldSelector) Gt(x int8) expr.Expr {
	return expr.Gt(f.Field, expr.Int8Value(x))
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Int8FieldSelector) Gte(x int8) expr.Expr {
	return expr.Gte(f.Field, expr.Int8Value(x))
}

// Lt matches if x is less than the field selected by f.
func (f Int8FieldSelector) Lt(x int8) expr.Expr {
	return expr.Lt(f.Field, expr.Int8Value(x))
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Int8FieldSelector) Lte(x int8) expr.Expr {
	return expr.Lte(f.Field, expr.Int8Value(x))
}

// Value returns a scalar that can be used as an expression.
func (f Int8FieldSelector) Value(x int8) *value.Value {
	return &value.Value{
		Type: value.Int8,
		Data: value.EncodeInt8(x),
	}
}

// Int16FieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type Int16FieldSelector struct {
	Field
}

// Int16Field creates a typed FieldSelector for fields of type int16.
func Int16Field(name string) Int16FieldSelector {
	return Int16FieldSelector{Field: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Int16FieldSelector) Eq(x int16) expr.Expr {
	return expr.Eq(f.Field, expr.Int16Value(x))
}

// Gt matches if x is greater than the field selected by f.
func (f Int16FieldSelector) Gt(x int16) expr.Expr {
	return expr.Gt(f.Field, expr.Int16Value(x))
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Int16FieldSelector) Gte(x int16) expr.Expr {
	return expr.Gte(f.Field, expr.Int16Value(x))
}

// Lt matches if x is less than the field selected by f.
func (f Int16FieldSelector) Lt(x int16) expr.Expr {
	return expr.Lt(f.Field, expr.Int16Value(x))
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Int16FieldSelector) Lte(x int16) expr.Expr {
	return expr.Lte(f.Field, expr.Int16Value(x))
}

// Value returns a scalar that can be used as an expression.
func (f Int16FieldSelector) Value(x int16) *value.Value {
	return &value.Value{
		Type: value.Int16,
		Data: value.EncodeInt16(x),
	}
}

// Int32FieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type Int32FieldSelector struct {
	Field
}

// Int32Field creates a typed FieldSelector for fields of type int32.
func Int32Field(name string) Int32FieldSelector {
	return Int32FieldSelector{Field: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Int32FieldSelector) Eq(x int32) expr.Expr {
	return expr.Eq(f.Field, expr.Int32Value(x))
}

// Gt matches if x is greater than the field selected by f.
func (f Int32FieldSelector) Gt(x int32) expr.Expr {
	return expr.Gt(f.Field, expr.Int32Value(x))
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Int32FieldSelector) Gte(x int32) expr.Expr {
	return expr.Gte(f.Field, expr.Int32Value(x))
}

// Lt matches if x is less than the field selected by f.
func (f Int32FieldSelector) Lt(x int32) expr.Expr {
	return expr.Lt(f.Field, expr.Int32Value(x))
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Int32FieldSelector) Lte(x int32) expr.Expr {
	return expr.Lte(f.Field, expr.Int32Value(x))
}

// Value returns a scalar that can be used as an expression.
func (f Int32FieldSelector) Value(x int32) *value.Value {
	return &value.Value{
		Type: value.Int32,
		Data: value.EncodeInt32(x),
	}
}

// Int64FieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type Int64FieldSelector struct {
	Field
}

// Int64Field creates a typed FieldSelector for fields of type int64.
func Int64Field(name string) Int64FieldSelector {
	return Int64FieldSelector{Field: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Int64FieldSelector) Eq(x int64) expr.Expr {
	return expr.Eq(f.Field, expr.Int64Value(x))
}

// Gt matches if x is greater than the field selected by f.
func (f Int64FieldSelector) Gt(x int64) expr.Expr {
	return expr.Gt(f.Field, expr.Int64Value(x))
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Int64FieldSelector) Gte(x int64) expr.Expr {
	return expr.Gte(f.Field, expr.Int64Value(x))
}

// Lt matches if x is less than the field selected by f.
func (f Int64FieldSelector) Lt(x int64) expr.Expr {
	return expr.Lt(f.Field, expr.Int64Value(x))
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Int64FieldSelector) Lte(x int64) expr.Expr {
	return expr.Lte(f.Field, expr.Int64Value(x))
}

// Value returns a scalar that can be used as an expression.
func (f Int64FieldSelector) Value(x int64) *value.Value {
	return &value.Value{
		Type: value.Int64,
		Data: value.EncodeInt64(x),
	}
}

// Float32FieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type Float32FieldSelector struct {
	Field
}

// Float32Field creates a typed FieldSelector for fields of type float32.
func Float32Field(name string) Float32FieldSelector {
	return Float32FieldSelector{Field: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Float32FieldSelector) Eq(x float32) expr.Expr {
	return expr.Eq(f.Field, expr.Float32Value(x))
}

// Gt matches if x is greater than the field selected by f.
func (f Float32FieldSelector) Gt(x float32) expr.Expr {
	return expr.Gt(f.Field, expr.Float32Value(x))
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Float32FieldSelector) Gte(x float32) expr.Expr {
	return expr.Gte(f.Field, expr.Float32Value(x))
}

// Lt matches if x is less than the field selected by f.
func (f Float32FieldSelector) Lt(x float32) expr.Expr {
	return expr.Lt(f.Field, expr.Float32Value(x))
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Float32FieldSelector) Lte(x float32) expr.Expr {
	return expr.Lte(f.Field, expr.Float32Value(x))
}

// Value returns a scalar that can be used as an expression.
func (f Float32FieldSelector) Value(x float32) *value.Value {
	return &value.Value{
		Type: value.Float32,
		Data: value.EncodeFloat32(x),
	}
}

// Float64FieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type Float64FieldSelector struct {
	Field
}

// Float64Field creates a typed FieldSelector for fields of type float64.
func Float64Field(name string) Float64FieldSelector {
	return Float64FieldSelector{Field: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Float64FieldSelector) Eq(x float64) expr.Expr {
	return expr.Eq(f.Field, expr.Float64Value(x))
}

// Gt matches if x is greater than the field selected by f.
func (f Float64FieldSelector) Gt(x float64) expr.Expr {
	return expr.Gt(f.Field, expr.Float64Value(x))
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Float64FieldSelector) Gte(x float64) expr.Expr {
	return expr.Gte(f.Field, expr.Float64Value(x))
}

// Lt matches if x is less than the field selected by f.
func (f Float64FieldSelector) Lt(x float64) expr.Expr {
	return expr.Lt(f.Field, expr.Float64Value(x))
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Float64FieldSelector) Lte(x float64) expr.Expr {
	return expr.Lte(f.Field, expr.Float64Value(x))
}

// Value returns a scalar that can be used as an expression.
func (f Float64FieldSelector) Value(x float64) *value.Value {
	return &value.Value{
		Type: value.Float64,
		Data: value.EncodeFloat64(x),
	}
}
