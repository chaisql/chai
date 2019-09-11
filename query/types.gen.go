package query

import (
	"github.com/asdine/genji/value"
)

// BytesFieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type BytesFieldSelector struct {
	FieldSelector
}

// BytesField creates a typed FieldSelector for fields of type []byte.
func BytesField(name string) BytesFieldSelector {
	return BytesFieldSelector{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f BytesFieldSelector) Eq(x []byte) Expr {
	return &eqMatcher{
		Field: f,
		Value: value.EncodeBytes(x),
	}
}

// Gt matches if x is greater than the field selected by f.
func (f BytesFieldSelector) Gt(x []byte) Expr {
	return &gtMatcher{
		Field: f,
		Value: value.EncodeBytes(x),
	}
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f BytesFieldSelector) Gte(x []byte) Expr {
	return &gteMatcher{
		Field: f,
		Value: value.EncodeBytes(x),
	}
}

// Lt matches if x is less than the field selected by f.
func (f BytesFieldSelector) Lt(x []byte) Expr {
	return &ltMatcher{
		Field: f,
		Value: value.EncodeBytes(x),
	}
}

// Lte matches if x is less than or equal to the field selected by f.
func (f BytesFieldSelector) Lte(x []byte) Expr {
	return &lteMatcher{
		Field: f,
		Value: value.EncodeBytes(x),
	}
}

// Value returns a scalar that can be used as an expression.
func (f BytesFieldSelector) Value(x []byte) *Scalar {
	return &Scalar{
		Type: value.Bytes,
		Data: value.EncodeBytes(x),
	}
}

// BytesValue is an expression that evaluates to itself.
func BytesValue(v []byte) Scalar {
	return Scalar{
		Type:  value.Bytes,
		Data:  value.EncodeBytes([]byte(v)),
		Value: v,
	}
}

// StringFieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type StringFieldSelector struct {
	FieldSelector
}

// StringField creates a typed FieldSelector for fields of type string.
func StringField(name string) StringFieldSelector {
	return StringFieldSelector{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f StringFieldSelector) Eq(x string) Expr {
	return &eqMatcher{
		Field: f,
		Value: value.EncodeString(x),
	}
}

// Gt matches if x is greater than the field selected by f.
func (f StringFieldSelector) Gt(x string) Expr {
	return &gtMatcher{
		Field: f,
		Value: value.EncodeString(x),
	}
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f StringFieldSelector) Gte(x string) Expr {
	return &gteMatcher{
		Field: f,
		Value: value.EncodeString(x),
	}
}

// Lt matches if x is less than the field selected by f.
func (f StringFieldSelector) Lt(x string) Expr {
	return &ltMatcher{
		Field: f,
		Value: value.EncodeString(x),
	}
}

// Lte matches if x is less than or equal to the field selected by f.
func (f StringFieldSelector) Lte(x string) Expr {
	return &lteMatcher{
		Field: f,
		Value: value.EncodeString(x),
	}
}

// Value returns a scalar that can be used as an expression.
func (f StringFieldSelector) Value(x string) *Scalar {
	return &Scalar{
		Type: value.String,
		Data: value.EncodeString(x),
	}
}

// StringValue is an expression that evaluates to itself.
func StringValue(v string) Scalar {
	return Scalar{
		Type:  value.String,
		Data:  value.EncodeString(string(v)),
		Value: v,
	}
}

// BoolFieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type BoolFieldSelector struct {
	FieldSelector
}

// BoolField creates a typed FieldSelector for fields of type bool.
func BoolField(name string) BoolFieldSelector {
	return BoolFieldSelector{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f BoolFieldSelector) Eq(x bool) Expr {
	return &eqMatcher{
		Field: f,
		Value: value.EncodeBool(x),
	}
}

// Gt matches if x is greater than the field selected by f.
func (f BoolFieldSelector) Gt(x bool) Expr {
	return &gtMatcher{
		Field: f,
		Value: value.EncodeBool(x),
	}
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f BoolFieldSelector) Gte(x bool) Expr {
	return &gteMatcher{
		Field: f,
		Value: value.EncodeBool(x),
	}
}

// Lt matches if x is less than the field selected by f.
func (f BoolFieldSelector) Lt(x bool) Expr {
	return &ltMatcher{
		Field: f,
		Value: value.EncodeBool(x),
	}
}

// Lte matches if x is less than or equal to the field selected by f.
func (f BoolFieldSelector) Lte(x bool) Expr {
	return &lteMatcher{
		Field: f,
		Value: value.EncodeBool(x),
	}
}

// Value returns a scalar that can be used as an expression.
func (f BoolFieldSelector) Value(x bool) *Scalar {
	return &Scalar{
		Type: value.Bool,
		Data: value.EncodeBool(x),
	}
}

// BoolValue is an expression that evaluates to itself.
func BoolValue(v bool) Scalar {
	return Scalar{
		Type:  value.Bool,
		Data:  value.EncodeBool(bool(v)),
		Value: v,
	}
}

// UintFieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type UintFieldSelector struct {
	FieldSelector
}

// UintField creates a typed FieldSelector for fields of type uint.
func UintField(name string) UintFieldSelector {
	return UintFieldSelector{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f UintFieldSelector) Eq(x uint) Expr {
	return &eqMatcher{
		Field: f,
		Value: value.EncodeUint(x),
	}
}

// Gt matches if x is greater than the field selected by f.
func (f UintFieldSelector) Gt(x uint) Expr {
	return &gtMatcher{
		Field: f,
		Value: value.EncodeUint(x),
	}
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f UintFieldSelector) Gte(x uint) Expr {
	return &gteMatcher{
		Field: f,
		Value: value.EncodeUint(x),
	}
}

// Lt matches if x is less than the field selected by f.
func (f UintFieldSelector) Lt(x uint) Expr {
	return &ltMatcher{
		Field: f,
		Value: value.EncodeUint(x),
	}
}

// Lte matches if x is less than or equal to the field selected by f.
func (f UintFieldSelector) Lte(x uint) Expr {
	return &lteMatcher{
		Field: f,
		Value: value.EncodeUint(x),
	}
}

// Value returns a scalar that can be used as an expression.
func (f UintFieldSelector) Value(x uint) *Scalar {
	return &Scalar{
		Type: value.Uint,
		Data: value.EncodeUint(x),
	}
}

// UintValue is an expression that evaluates to itself.
func UintValue(v uint) Scalar {
	return Scalar{
		Type:  value.Uint,
		Data:  value.EncodeUint(uint(v)),
		Value: v,
	}
}

// Uint8FieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type Uint8FieldSelector struct {
	FieldSelector
}

// Uint8Field creates a typed FieldSelector for fields of type uint8.
func Uint8Field(name string) Uint8FieldSelector {
	return Uint8FieldSelector{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Uint8FieldSelector) Eq(x uint8) Expr {
	return &eqMatcher{
		Field: f,
		Value: value.EncodeUint8(x),
	}
}

// Gt matches if x is greater than the field selected by f.
func (f Uint8FieldSelector) Gt(x uint8) Expr {
	return &gtMatcher{
		Field: f,
		Value: value.EncodeUint8(x),
	}
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Uint8FieldSelector) Gte(x uint8) Expr {
	return &gteMatcher{
		Field: f,
		Value: value.EncodeUint8(x),
	}
}

// Lt matches if x is less than the field selected by f.
func (f Uint8FieldSelector) Lt(x uint8) Expr {
	return &ltMatcher{
		Field: f,
		Value: value.EncodeUint8(x),
	}
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Uint8FieldSelector) Lte(x uint8) Expr {
	return &lteMatcher{
		Field: f,
		Value: value.EncodeUint8(x),
	}
}

// Value returns a scalar that can be used as an expression.
func (f Uint8FieldSelector) Value(x uint8) *Scalar {
	return &Scalar{
		Type: value.Uint8,
		Data: value.EncodeUint8(x),
	}
}

// Uint8Value is an expression that evaluates to itself.
func Uint8Value(v uint8) Scalar {
	return Scalar{
		Type:  value.Uint8,
		Data:  value.EncodeUint8(uint8(v)),
		Value: v,
	}
}

// Uint16FieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type Uint16FieldSelector struct {
	FieldSelector
}

// Uint16Field creates a typed FieldSelector for fields of type uint16.
func Uint16Field(name string) Uint16FieldSelector {
	return Uint16FieldSelector{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Uint16FieldSelector) Eq(x uint16) Expr {
	return &eqMatcher{
		Field: f,
		Value: value.EncodeUint16(x),
	}
}

// Gt matches if x is greater than the field selected by f.
func (f Uint16FieldSelector) Gt(x uint16) Expr {
	return &gtMatcher{
		Field: f,
		Value: value.EncodeUint16(x),
	}
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Uint16FieldSelector) Gte(x uint16) Expr {
	return &gteMatcher{
		Field: f,
		Value: value.EncodeUint16(x),
	}
}

// Lt matches if x is less than the field selected by f.
func (f Uint16FieldSelector) Lt(x uint16) Expr {
	return &ltMatcher{
		Field: f,
		Value: value.EncodeUint16(x),
	}
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Uint16FieldSelector) Lte(x uint16) Expr {
	return &lteMatcher{
		Field: f,
		Value: value.EncodeUint16(x),
	}
}

// Value returns a scalar that can be used as an expression.
func (f Uint16FieldSelector) Value(x uint16) *Scalar {
	return &Scalar{
		Type: value.Uint16,
		Data: value.EncodeUint16(x),
	}
}

// Uint16Value is an expression that evaluates to itself.
func Uint16Value(v uint16) Scalar {
	return Scalar{
		Type:  value.Uint16,
		Data:  value.EncodeUint16(uint16(v)),
		Value: v,
	}
}

// Uint32FieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type Uint32FieldSelector struct {
	FieldSelector
}

// Uint32Field creates a typed FieldSelector for fields of type uint32.
func Uint32Field(name string) Uint32FieldSelector {
	return Uint32FieldSelector{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Uint32FieldSelector) Eq(x uint32) Expr {
	return &eqMatcher{
		Field: f,
		Value: value.EncodeUint32(x),
	}
}

// Gt matches if x is greater than the field selected by f.
func (f Uint32FieldSelector) Gt(x uint32) Expr {
	return &gtMatcher{
		Field: f,
		Value: value.EncodeUint32(x),
	}
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Uint32FieldSelector) Gte(x uint32) Expr {
	return &gteMatcher{
		Field: f,
		Value: value.EncodeUint32(x),
	}
}

// Lt matches if x is less than the field selected by f.
func (f Uint32FieldSelector) Lt(x uint32) Expr {
	return &ltMatcher{
		Field: f,
		Value: value.EncodeUint32(x),
	}
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Uint32FieldSelector) Lte(x uint32) Expr {
	return &lteMatcher{
		Field: f,
		Value: value.EncodeUint32(x),
	}
}

// Value returns a scalar that can be used as an expression.
func (f Uint32FieldSelector) Value(x uint32) *Scalar {
	return &Scalar{
		Type: value.Uint32,
		Data: value.EncodeUint32(x),
	}
}

// Uint32Value is an expression that evaluates to itself.
func Uint32Value(v uint32) Scalar {
	return Scalar{
		Type:  value.Uint32,
		Data:  value.EncodeUint32(uint32(v)),
		Value: v,
	}
}

// Uint64FieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type Uint64FieldSelector struct {
	FieldSelector
}

// Uint64Field creates a typed FieldSelector for fields of type uint64.
func Uint64Field(name string) Uint64FieldSelector {
	return Uint64FieldSelector{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Uint64FieldSelector) Eq(x uint64) Expr {
	return &eqMatcher{
		Field: f,
		Value: value.EncodeUint64(x),
	}
}

// Gt matches if x is greater than the field selected by f.
func (f Uint64FieldSelector) Gt(x uint64) Expr {
	return &gtMatcher{
		Field: f,
		Value: value.EncodeUint64(x),
	}
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Uint64FieldSelector) Gte(x uint64) Expr {
	return &gteMatcher{
		Field: f,
		Value: value.EncodeUint64(x),
	}
}

// Lt matches if x is less than the field selected by f.
func (f Uint64FieldSelector) Lt(x uint64) Expr {
	return &ltMatcher{
		Field: f,
		Value: value.EncodeUint64(x),
	}
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Uint64FieldSelector) Lte(x uint64) Expr {
	return &lteMatcher{
		Field: f,
		Value: value.EncodeUint64(x),
	}
}

// Value returns a scalar that can be used as an expression.
func (f Uint64FieldSelector) Value(x uint64) *Scalar {
	return &Scalar{
		Type: value.Uint64,
		Data: value.EncodeUint64(x),
	}
}

// Uint64Value is an expression that evaluates to itself.
func Uint64Value(v uint64) Scalar {
	return Scalar{
		Type:  value.Uint64,
		Data:  value.EncodeUint64(uint64(v)),
		Value: v,
	}
}

// IntFieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type IntFieldSelector struct {
	FieldSelector
}

// IntField creates a typed FieldSelector for fields of type int.
func IntField(name string) IntFieldSelector {
	return IntFieldSelector{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f IntFieldSelector) Eq(x int) Expr {
	return &eqMatcher{
		Field: f,
		Value: value.EncodeInt(x),
	}
}

// Gt matches if x is greater than the field selected by f.
func (f IntFieldSelector) Gt(x int) Expr {
	return &gtMatcher{
		Field: f,
		Value: value.EncodeInt(x),
	}
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f IntFieldSelector) Gte(x int) Expr {
	return &gteMatcher{
		Field: f,
		Value: value.EncodeInt(x),
	}
}

// Lt matches if x is less than the field selected by f.
func (f IntFieldSelector) Lt(x int) Expr {
	return &ltMatcher{
		Field: f,
		Value: value.EncodeInt(x),
	}
}

// Lte matches if x is less than or equal to the field selected by f.
func (f IntFieldSelector) Lte(x int) Expr {
	return &lteMatcher{
		Field: f,
		Value: value.EncodeInt(x),
	}
}

// Value returns a scalar that can be used as an expression.
func (f IntFieldSelector) Value(x int) *Scalar {
	return &Scalar{
		Type: value.Int,
		Data: value.EncodeInt(x),
	}
}

// IntValue is an expression that evaluates to itself.
func IntValue(v int) Scalar {
	return Scalar{
		Type:  value.Int,
		Data:  value.EncodeInt(int(v)),
		Value: v,
	}
}

// Int8FieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type Int8FieldSelector struct {
	FieldSelector
}

// Int8Field creates a typed FieldSelector for fields of type int8.
func Int8Field(name string) Int8FieldSelector {
	return Int8FieldSelector{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Int8FieldSelector) Eq(x int8) Expr {
	return &eqMatcher{
		Field: f,
		Value: value.EncodeInt8(x),
	}
}

// Gt matches if x is greater than the field selected by f.
func (f Int8FieldSelector) Gt(x int8) Expr {
	return &gtMatcher{
		Field: f,
		Value: value.EncodeInt8(x),
	}
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Int8FieldSelector) Gte(x int8) Expr {
	return &gteMatcher{
		Field: f,
		Value: value.EncodeInt8(x),
	}
}

// Lt matches if x is less than the field selected by f.
func (f Int8FieldSelector) Lt(x int8) Expr {
	return &ltMatcher{
		Field: f,
		Value: value.EncodeInt8(x),
	}
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Int8FieldSelector) Lte(x int8) Expr {
	return &lteMatcher{
		Field: f,
		Value: value.EncodeInt8(x),
	}
}

// Value returns a scalar that can be used as an expression.
func (f Int8FieldSelector) Value(x int8) *Scalar {
	return &Scalar{
		Type: value.Int8,
		Data: value.EncodeInt8(x),
	}
}

// Int8Value is an expression that evaluates to itself.
func Int8Value(v int8) Scalar {
	return Scalar{
		Type:  value.Int8,
		Data:  value.EncodeInt8(int8(v)),
		Value: v,
	}
}

// Int16FieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type Int16FieldSelector struct {
	FieldSelector
}

// Int16Field creates a typed FieldSelector for fields of type int16.
func Int16Field(name string) Int16FieldSelector {
	return Int16FieldSelector{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Int16FieldSelector) Eq(x int16) Expr {
	return &eqMatcher{
		Field: f,
		Value: value.EncodeInt16(x),
	}
}

// Gt matches if x is greater than the field selected by f.
func (f Int16FieldSelector) Gt(x int16) Expr {
	return &gtMatcher{
		Field: f,
		Value: value.EncodeInt16(x),
	}
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Int16FieldSelector) Gte(x int16) Expr {
	return &gteMatcher{
		Field: f,
		Value: value.EncodeInt16(x),
	}
}

// Lt matches if x is less than the field selected by f.
func (f Int16FieldSelector) Lt(x int16) Expr {
	return &ltMatcher{
		Field: f,
		Value: value.EncodeInt16(x),
	}
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Int16FieldSelector) Lte(x int16) Expr {
	return &lteMatcher{
		Field: f,
		Value: value.EncodeInt16(x),
	}
}

// Value returns a scalar that can be used as an expression.
func (f Int16FieldSelector) Value(x int16) *Scalar {
	return &Scalar{
		Type: value.Int16,
		Data: value.EncodeInt16(x),
	}
}

// Int16Value is an expression that evaluates to itself.
func Int16Value(v int16) Scalar {
	return Scalar{
		Type:  value.Int16,
		Data:  value.EncodeInt16(int16(v)),
		Value: v,
	}
}

// Int32FieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type Int32FieldSelector struct {
	FieldSelector
}

// Int32Field creates a typed FieldSelector for fields of type int32.
func Int32Field(name string) Int32FieldSelector {
	return Int32FieldSelector{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Int32FieldSelector) Eq(x int32) Expr {
	return &eqMatcher{
		Field: f,
		Value: value.EncodeInt32(x),
	}
}

// Gt matches if x is greater than the field selected by f.
func (f Int32FieldSelector) Gt(x int32) Expr {
	return &gtMatcher{
		Field: f,
		Value: value.EncodeInt32(x),
	}
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Int32FieldSelector) Gte(x int32) Expr {
	return &gteMatcher{
		Field: f,
		Value: value.EncodeInt32(x),
	}
}

// Lt matches if x is less than the field selected by f.
func (f Int32FieldSelector) Lt(x int32) Expr {
	return &ltMatcher{
		Field: f,
		Value: value.EncodeInt32(x),
	}
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Int32FieldSelector) Lte(x int32) Expr {
	return &lteMatcher{
		Field: f,
		Value: value.EncodeInt32(x),
	}
}

// Value returns a scalar that can be used as an expression.
func (f Int32FieldSelector) Value(x int32) *Scalar {
	return &Scalar{
		Type: value.Int32,
		Data: value.EncodeInt32(x),
	}
}

// Int32Value is an expression that evaluates to itself.
func Int32Value(v int32) Scalar {
	return Scalar{
		Type:  value.Int32,
		Data:  value.EncodeInt32(int32(v)),
		Value: v,
	}
}

// Int64FieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type Int64FieldSelector struct {
	FieldSelector
}

// Int64Field creates a typed FieldSelector for fields of type int64.
func Int64Field(name string) Int64FieldSelector {
	return Int64FieldSelector{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Int64FieldSelector) Eq(x int64) Expr {
	return &eqMatcher{
		Field: f,
		Value: value.EncodeInt64(x),
	}
}

// Gt matches if x is greater than the field selected by f.
func (f Int64FieldSelector) Gt(x int64) Expr {
	return &gtMatcher{
		Field: f,
		Value: value.EncodeInt64(x),
	}
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Int64FieldSelector) Gte(x int64) Expr {
	return &gteMatcher{
		Field: f,
		Value: value.EncodeInt64(x),
	}
}

// Lt matches if x is less than the field selected by f.
func (f Int64FieldSelector) Lt(x int64) Expr {
	return &ltMatcher{
		Field: f,
		Value: value.EncodeInt64(x),
	}
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Int64FieldSelector) Lte(x int64) Expr {
	return &lteMatcher{
		Field: f,
		Value: value.EncodeInt64(x),
	}
}

// Value returns a scalar that can be used as an expression.
func (f Int64FieldSelector) Value(x int64) *Scalar {
	return &Scalar{
		Type: value.Int64,
		Data: value.EncodeInt64(x),
	}
}

// Int64Value is an expression that evaluates to itself.
func Int64Value(v int64) Scalar {
	return Scalar{
		Type:  value.Int64,
		Data:  value.EncodeInt64(int64(v)),
		Value: v,
	}
}

// Float32FieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type Float32FieldSelector struct {
	FieldSelector
}

// Float32Field creates a typed FieldSelector for fields of type float32.
func Float32Field(name string) Float32FieldSelector {
	return Float32FieldSelector{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Float32FieldSelector) Eq(x float32) Expr {
	return &eqMatcher{
		Field: f,
		Value: value.EncodeFloat32(x),
	}
}

// Gt matches if x is greater than the field selected by f.
func (f Float32FieldSelector) Gt(x float32) Expr {
	return &gtMatcher{
		Field: f,
		Value: value.EncodeFloat32(x),
	}
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Float32FieldSelector) Gte(x float32) Expr {
	return &gteMatcher{
		Field: f,
		Value: value.EncodeFloat32(x),
	}
}

// Lt matches if x is less than the field selected by f.
func (f Float32FieldSelector) Lt(x float32) Expr {
	return &ltMatcher{
		Field: f,
		Value: value.EncodeFloat32(x),
	}
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Float32FieldSelector) Lte(x float32) Expr {
	return &lteMatcher{
		Field: f,
		Value: value.EncodeFloat32(x),
	}
}

// Value returns a scalar that can be used as an expression.
func (f Float32FieldSelector) Value(x float32) *Scalar {
	return &Scalar{
		Type: value.Float32,
		Data: value.EncodeFloat32(x),
	}
}

// Float32Value is an expression that evaluates to itself.
func Float32Value(v float32) Scalar {
	return Scalar{
		Type:  value.Float32,
		Data:  value.EncodeFloat32(float32(v)),
		Value: v,
	}
}

// Float64FieldSelector is a type safe field selector that allows to compare values with fields
// based on their types.
type Float64FieldSelector struct {
	FieldSelector
}

// Float64Field creates a typed FieldSelector for fields of type float64.
func Float64Field(name string) Float64FieldSelector {
	return Float64FieldSelector{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Float64FieldSelector) Eq(x float64) Expr {
	return &eqMatcher{
		Field: f,
		Value: value.EncodeFloat64(x),
	}
}

// Gt matches if x is greater than the field selected by f.
func (f Float64FieldSelector) Gt(x float64) Expr {
	return &gtMatcher{
		Field: f,
		Value: value.EncodeFloat64(x),
	}
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Float64FieldSelector) Gte(x float64) Expr {
	return &gteMatcher{
		Field: f,
		Value: value.EncodeFloat64(x),
	}
}

// Lt matches if x is less than the field selected by f.
func (f Float64FieldSelector) Lt(x float64) Expr {
	return &ltMatcher{
		Field: f,
		Value: value.EncodeFloat64(x),
	}
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Float64FieldSelector) Lte(x float64) Expr {
	return &lteMatcher{
		Field: f,
		Value: value.EncodeFloat64(x),
	}
}

// Value returns a scalar that can be used as an expression.
func (f Float64FieldSelector) Value(x float64) *Scalar {
	return &Scalar{
		Type: value.Float64,
		Data: value.EncodeFloat64(x),
	}
}

// Float64Value is an expression that evaluates to itself.
func Float64Value(v float64) Scalar {
	return Scalar{
		Type:  value.Float64,
		Data:  value.EncodeFloat64(float64(v)),
		Value: v,
	}
}
