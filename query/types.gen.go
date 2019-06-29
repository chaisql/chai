
package query

import (
	"github.com/asdine/genji/field"
)

// EqBytes matches if x is equal to the field selected by f.
func EqBytes(f FieldSelector, x []byte) Expr {
	return &eqMatcher{
		Field: f,
		Value: field.EncodeBytes(x),
	}
}

// GtBytes matches if x is greater than the field selected by f.
func GtBytes(f FieldSelector, x []byte) Expr {
	return &gtMatcher{
		Field: f,
		Value: field.EncodeBytes(x),
	}
}

// GteBytes matches if x is greater than or equal to the field selected by f.
func GteBytes(f FieldSelector, x []byte) Expr {
	return &gteMatcher{
		Field: f,
		Value: field.EncodeBytes(x),
	}
}

// LtBytes matches if x is less than the field selected by f.
func LtBytes(f FieldSelector, x []byte) Expr {
	return &ltMatcher{
		Field: f,
		Value: field.EncodeBytes(x),
	}
}

// LteBytes matches if x is less than or equal to the field selected by f.
func LteBytes(f FieldSelector, x []byte) Expr {
	return &lteMatcher{
		Field: f,
		Value: field.EncodeBytes(x),
	}
}

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
	return EqBytes(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f BytesFieldSelector) Gt(x []byte) Expr {
	return GtBytes(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f BytesFieldSelector) Gte(x []byte) Expr {
	return GteBytes(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f BytesFieldSelector) Lt(x []byte) Expr {
	return LtBytes(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f BytesFieldSelector) Lte(x []byte) Expr {
	return LteBytes(f.FieldSelector, x)
}

// Value returns a scalar that can be used as an expression.
func (f BytesFieldSelector) Value(x []byte) *Scalar {
	return &Scalar{
		Type: field.Bytes,
		Data: field.EncodeBytes(x),
	}
}

// BytesValue is an expression that evaluates to itself.
type BytesValue []byte

// Eval implements the Expr interface. It returns a scalar after encoding v to
// the right type.
func (v BytesValue) Eval(EvalContext) (Scalar, error) {
	return Scalar{
		Type: field.Bytes,
		Data: field.EncodeBytes([]byte(v)),
	}, nil
}

// EqString matches if x is equal to the field selected by f.
func EqString(f FieldSelector, x string) Expr {
	return &eqMatcher{
		Field: f,
		Value: field.EncodeString(x),
	}
}

// GtString matches if x is greater than the field selected by f.
func GtString(f FieldSelector, x string) Expr {
	return &gtMatcher{
		Field: f,
		Value: field.EncodeString(x),
	}
}

// GteString matches if x is greater than or equal to the field selected by f.
func GteString(f FieldSelector, x string) Expr {
	return &gteMatcher{
		Field: f,
		Value: field.EncodeString(x),
	}
}

// LtString matches if x is less than the field selected by f.
func LtString(f FieldSelector, x string) Expr {
	return &ltMatcher{
		Field: f,
		Value: field.EncodeString(x),
	}
}

// LteString matches if x is less than or equal to the field selected by f.
func LteString(f FieldSelector, x string) Expr {
	return &lteMatcher{
		Field: f,
		Value: field.EncodeString(x),
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
	return EqString(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f StringFieldSelector) Gt(x string) Expr {
	return GtString(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f StringFieldSelector) Gte(x string) Expr {
	return GteString(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f StringFieldSelector) Lt(x string) Expr {
	return LtString(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f StringFieldSelector) Lte(x string) Expr {
	return LteString(f.FieldSelector, x)
}

// Value returns a scalar that can be used as an expression.
func (f StringFieldSelector) Value(x string) *Scalar {
	return &Scalar{
		Type: field.String,
		Data: field.EncodeString(x),
	}
}

// StringValue is an expression that evaluates to itself.
type StringValue string

// Eval implements the Expr interface. It returns a scalar after encoding v to
// the right type.
func (v StringValue) Eval(EvalContext) (Scalar, error) {
	return Scalar{
		Type: field.String,
		Data: field.EncodeString(string(v)),
	}, nil
}

// EqBool matches if x is equal to the field selected by f.
func EqBool(f FieldSelector, x bool) Expr {
	return &eqMatcher{
		Field: f,
		Value: field.EncodeBool(x),
	}
}

// GtBool matches if x is greater than the field selected by f.
func GtBool(f FieldSelector, x bool) Expr {
	return &gtMatcher{
		Field: f,
		Value: field.EncodeBool(x),
	}
}

// GteBool matches if x is greater than or equal to the field selected by f.
func GteBool(f FieldSelector, x bool) Expr {
	return &gteMatcher{
		Field: f,
		Value: field.EncodeBool(x),
	}
}

// LtBool matches if x is less than the field selected by f.
func LtBool(f FieldSelector, x bool) Expr {
	return &ltMatcher{
		Field: f,
		Value: field.EncodeBool(x),
	}
}

// LteBool matches if x is less than or equal to the field selected by f.
func LteBool(f FieldSelector, x bool) Expr {
	return &lteMatcher{
		Field: f,
		Value: field.EncodeBool(x),
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
	return EqBool(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f BoolFieldSelector) Gt(x bool) Expr {
	return GtBool(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f BoolFieldSelector) Gte(x bool) Expr {
	return GteBool(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f BoolFieldSelector) Lt(x bool) Expr {
	return LtBool(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f BoolFieldSelector) Lte(x bool) Expr {
	return LteBool(f.FieldSelector, x)
}

// Value returns a scalar that can be used as an expression.
func (f BoolFieldSelector) Value(x bool) *Scalar {
	return &Scalar{
		Type: field.Bool,
		Data: field.EncodeBool(x),
	}
}

// BoolValue is an expression that evaluates to itself.
type BoolValue bool

// Eval implements the Expr interface. It returns a scalar after encoding v to
// the right type.
func (v BoolValue) Eval(EvalContext) (Scalar, error) {
	return Scalar{
		Type: field.Bool,
		Data: field.EncodeBool(bool(v)),
	}, nil
}

// EqUint matches if x is equal to the field selected by f.
func EqUint(f FieldSelector, x uint) Expr {
	return &eqMatcher{
		Field: f,
		Value: field.EncodeUint(x),
	}
}

// GtUint matches if x is greater than the field selected by f.
func GtUint(f FieldSelector, x uint) Expr {
	return &gtMatcher{
		Field: f,
		Value: field.EncodeUint(x),
	}
}

// GteUint matches if x is greater than or equal to the field selected by f.
func GteUint(f FieldSelector, x uint) Expr {
	return &gteMatcher{
		Field: f,
		Value: field.EncodeUint(x),
	}
}

// LtUint matches if x is less than the field selected by f.
func LtUint(f FieldSelector, x uint) Expr {
	return &ltMatcher{
		Field: f,
		Value: field.EncodeUint(x),
	}
}

// LteUint matches if x is less than or equal to the field selected by f.
func LteUint(f FieldSelector, x uint) Expr {
	return &lteMatcher{
		Field: f,
		Value: field.EncodeUint(x),
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
	return EqUint(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f UintFieldSelector) Gt(x uint) Expr {
	return GtUint(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f UintFieldSelector) Gte(x uint) Expr {
	return GteUint(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f UintFieldSelector) Lt(x uint) Expr {
	return LtUint(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f UintFieldSelector) Lte(x uint) Expr {
	return LteUint(f.FieldSelector, x)
}

// Value returns a scalar that can be used as an expression.
func (f UintFieldSelector) Value(x uint) *Scalar {
	return &Scalar{
		Type: field.Uint,
		Data: field.EncodeUint(x),
	}
}

// UintValue is an expression that evaluates to itself.
type UintValue uint

// Eval implements the Expr interface. It returns a scalar after encoding v to
// the right type.
func (v UintValue) Eval(EvalContext) (Scalar, error) {
	return Scalar{
		Type: field.Uint,
		Data: field.EncodeUint(uint(v)),
	}, nil
}

// EqUint8 matches if x is equal to the field selected by f.
func EqUint8(f FieldSelector, x uint8) Expr {
	return &eqMatcher{
		Field: f,
		Value: field.EncodeUint8(x),
	}
}

// GtUint8 matches if x is greater than the field selected by f.
func GtUint8(f FieldSelector, x uint8) Expr {
	return &gtMatcher{
		Field: f,
		Value: field.EncodeUint8(x),
	}
}

// GteUint8 matches if x is greater than or equal to the field selected by f.
func GteUint8(f FieldSelector, x uint8) Expr {
	return &gteMatcher{
		Field: f,
		Value: field.EncodeUint8(x),
	}
}

// LtUint8 matches if x is less than the field selected by f.
func LtUint8(f FieldSelector, x uint8) Expr {
	return &ltMatcher{
		Field: f,
		Value: field.EncodeUint8(x),
	}
}

// LteUint8 matches if x is less than or equal to the field selected by f.
func LteUint8(f FieldSelector, x uint8) Expr {
	return &lteMatcher{
		Field: f,
		Value: field.EncodeUint8(x),
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
	return EqUint8(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f Uint8FieldSelector) Gt(x uint8) Expr {
	return GtUint8(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Uint8FieldSelector) Gte(x uint8) Expr {
	return GteUint8(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f Uint8FieldSelector) Lt(x uint8) Expr {
	return LtUint8(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Uint8FieldSelector) Lte(x uint8) Expr {
	return LteUint8(f.FieldSelector, x)
}

// Value returns a scalar that can be used as an expression.
func (f Uint8FieldSelector) Value(x uint8) *Scalar {
	return &Scalar{
		Type: field.Uint8,
		Data: field.EncodeUint8(x),
	}
}

// Uint8Value is an expression that evaluates to itself.
type Uint8Value uint8

// Eval implements the Expr interface. It returns a scalar after encoding v to
// the right type.
func (v Uint8Value) Eval(EvalContext) (Scalar, error) {
	return Scalar{
		Type: field.Uint8,
		Data: field.EncodeUint8(uint8(v)),
	}, nil
}

// EqUint16 matches if x is equal to the field selected by f.
func EqUint16(f FieldSelector, x uint16) Expr {
	return &eqMatcher{
		Field: f,
		Value: field.EncodeUint16(x),
	}
}

// GtUint16 matches if x is greater than the field selected by f.
func GtUint16(f FieldSelector, x uint16) Expr {
	return &gtMatcher{
		Field: f,
		Value: field.EncodeUint16(x),
	}
}

// GteUint16 matches if x is greater than or equal to the field selected by f.
func GteUint16(f FieldSelector, x uint16) Expr {
	return &gteMatcher{
		Field: f,
		Value: field.EncodeUint16(x),
	}
}

// LtUint16 matches if x is less than the field selected by f.
func LtUint16(f FieldSelector, x uint16) Expr {
	return &ltMatcher{
		Field: f,
		Value: field.EncodeUint16(x),
	}
}

// LteUint16 matches if x is less than or equal to the field selected by f.
func LteUint16(f FieldSelector, x uint16) Expr {
	return &lteMatcher{
		Field: f,
		Value: field.EncodeUint16(x),
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
	return EqUint16(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f Uint16FieldSelector) Gt(x uint16) Expr {
	return GtUint16(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Uint16FieldSelector) Gte(x uint16) Expr {
	return GteUint16(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f Uint16FieldSelector) Lt(x uint16) Expr {
	return LtUint16(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Uint16FieldSelector) Lte(x uint16) Expr {
	return LteUint16(f.FieldSelector, x)
}

// Value returns a scalar that can be used as an expression.
func (f Uint16FieldSelector) Value(x uint16) *Scalar {
	return &Scalar{
		Type: field.Uint16,
		Data: field.EncodeUint16(x),
	}
}

// Uint16Value is an expression that evaluates to itself.
type Uint16Value uint16

// Eval implements the Expr interface. It returns a scalar after encoding v to
// the right type.
func (v Uint16Value) Eval(EvalContext) (Scalar, error) {
	return Scalar{
		Type: field.Uint16,
		Data: field.EncodeUint16(uint16(v)),
	}, nil
}

// EqUint32 matches if x is equal to the field selected by f.
func EqUint32(f FieldSelector, x uint32) Expr {
	return &eqMatcher{
		Field: f,
		Value: field.EncodeUint32(x),
	}
}

// GtUint32 matches if x is greater than the field selected by f.
func GtUint32(f FieldSelector, x uint32) Expr {
	return &gtMatcher{
		Field: f,
		Value: field.EncodeUint32(x),
	}
}

// GteUint32 matches if x is greater than or equal to the field selected by f.
func GteUint32(f FieldSelector, x uint32) Expr {
	return &gteMatcher{
		Field: f,
		Value: field.EncodeUint32(x),
	}
}

// LtUint32 matches if x is less than the field selected by f.
func LtUint32(f FieldSelector, x uint32) Expr {
	return &ltMatcher{
		Field: f,
		Value: field.EncodeUint32(x),
	}
}

// LteUint32 matches if x is less than or equal to the field selected by f.
func LteUint32(f FieldSelector, x uint32) Expr {
	return &lteMatcher{
		Field: f,
		Value: field.EncodeUint32(x),
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
	return EqUint32(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f Uint32FieldSelector) Gt(x uint32) Expr {
	return GtUint32(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Uint32FieldSelector) Gte(x uint32) Expr {
	return GteUint32(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f Uint32FieldSelector) Lt(x uint32) Expr {
	return LtUint32(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Uint32FieldSelector) Lte(x uint32) Expr {
	return LteUint32(f.FieldSelector, x)
}

// Value returns a scalar that can be used as an expression.
func (f Uint32FieldSelector) Value(x uint32) *Scalar {
	return &Scalar{
		Type: field.Uint32,
		Data: field.EncodeUint32(x),
	}
}

// Uint32Value is an expression that evaluates to itself.
type Uint32Value uint32

// Eval implements the Expr interface. It returns a scalar after encoding v to
// the right type.
func (v Uint32Value) Eval(EvalContext) (Scalar, error) {
	return Scalar{
		Type: field.Uint32,
		Data: field.EncodeUint32(uint32(v)),
	}, nil
}

// EqUint64 matches if x is equal to the field selected by f.
func EqUint64(f FieldSelector, x uint64) Expr {
	return &eqMatcher{
		Field: f,
		Value: field.EncodeUint64(x),
	}
}

// GtUint64 matches if x is greater than the field selected by f.
func GtUint64(f FieldSelector, x uint64) Expr {
	return &gtMatcher{
		Field: f,
		Value: field.EncodeUint64(x),
	}
}

// GteUint64 matches if x is greater than or equal to the field selected by f.
func GteUint64(f FieldSelector, x uint64) Expr {
	return &gteMatcher{
		Field: f,
		Value: field.EncodeUint64(x),
	}
}

// LtUint64 matches if x is less than the field selected by f.
func LtUint64(f FieldSelector, x uint64) Expr {
	return &ltMatcher{
		Field: f,
		Value: field.EncodeUint64(x),
	}
}

// LteUint64 matches if x is less than or equal to the field selected by f.
func LteUint64(f FieldSelector, x uint64) Expr {
	return &lteMatcher{
		Field: f,
		Value: field.EncodeUint64(x),
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
	return EqUint64(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f Uint64FieldSelector) Gt(x uint64) Expr {
	return GtUint64(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Uint64FieldSelector) Gte(x uint64) Expr {
	return GteUint64(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f Uint64FieldSelector) Lt(x uint64) Expr {
	return LtUint64(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Uint64FieldSelector) Lte(x uint64) Expr {
	return LteUint64(f.FieldSelector, x)
}

// Value returns a scalar that can be used as an expression.
func (f Uint64FieldSelector) Value(x uint64) *Scalar {
	return &Scalar{
		Type: field.Uint64,
		Data: field.EncodeUint64(x),
	}
}

// Uint64Value is an expression that evaluates to itself.
type Uint64Value uint64

// Eval implements the Expr interface. It returns a scalar after encoding v to
// the right type.
func (v Uint64Value) Eval(EvalContext) (Scalar, error) {
	return Scalar{
		Type: field.Uint64,
		Data: field.EncodeUint64(uint64(v)),
	}, nil
}

// EqInt matches if x is equal to the field selected by f.
func EqInt(f FieldSelector, x int) Expr {
	return &eqMatcher{
		Field: f,
		Value: field.EncodeInt(x),
	}
}

// GtInt matches if x is greater than the field selected by f.
func GtInt(f FieldSelector, x int) Expr {
	return &gtMatcher{
		Field: f,
		Value: field.EncodeInt(x),
	}
}

// GteInt matches if x is greater than or equal to the field selected by f.
func GteInt(f FieldSelector, x int) Expr {
	return &gteMatcher{
		Field: f,
		Value: field.EncodeInt(x),
	}
}

// LtInt matches if x is less than the field selected by f.
func LtInt(f FieldSelector, x int) Expr {
	return &ltMatcher{
		Field: f,
		Value: field.EncodeInt(x),
	}
}

// LteInt matches if x is less than or equal to the field selected by f.
func LteInt(f FieldSelector, x int) Expr {
	return &lteMatcher{
		Field: f,
		Value: field.EncodeInt(x),
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
	return EqInt(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f IntFieldSelector) Gt(x int) Expr {
	return GtInt(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f IntFieldSelector) Gte(x int) Expr {
	return GteInt(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f IntFieldSelector) Lt(x int) Expr {
	return LtInt(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f IntFieldSelector) Lte(x int) Expr {
	return LteInt(f.FieldSelector, x)
}

// Value returns a scalar that can be used as an expression.
func (f IntFieldSelector) Value(x int) *Scalar {
	return &Scalar{
		Type: field.Int,
		Data: field.EncodeInt(x),
	}
}

// IntValue is an expression that evaluates to itself.
type IntValue int

// Eval implements the Expr interface. It returns a scalar after encoding v to
// the right type.
func (v IntValue) Eval(EvalContext) (Scalar, error) {
	return Scalar{
		Type: field.Int,
		Data: field.EncodeInt(int(v)),
	}, nil
}

// EqInt8 matches if x is equal to the field selected by f.
func EqInt8(f FieldSelector, x int8) Expr {
	return &eqMatcher{
		Field: f,
		Value: field.EncodeInt8(x),
	}
}

// GtInt8 matches if x is greater than the field selected by f.
func GtInt8(f FieldSelector, x int8) Expr {
	return &gtMatcher{
		Field: f,
		Value: field.EncodeInt8(x),
	}
}

// GteInt8 matches if x is greater than or equal to the field selected by f.
func GteInt8(f FieldSelector, x int8) Expr {
	return &gteMatcher{
		Field: f,
		Value: field.EncodeInt8(x),
	}
}

// LtInt8 matches if x is less than the field selected by f.
func LtInt8(f FieldSelector, x int8) Expr {
	return &ltMatcher{
		Field: f,
		Value: field.EncodeInt8(x),
	}
}

// LteInt8 matches if x is less than or equal to the field selected by f.
func LteInt8(f FieldSelector, x int8) Expr {
	return &lteMatcher{
		Field: f,
		Value: field.EncodeInt8(x),
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
	return EqInt8(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f Int8FieldSelector) Gt(x int8) Expr {
	return GtInt8(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Int8FieldSelector) Gte(x int8) Expr {
	return GteInt8(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f Int8FieldSelector) Lt(x int8) Expr {
	return LtInt8(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Int8FieldSelector) Lte(x int8) Expr {
	return LteInt8(f.FieldSelector, x)
}

// Value returns a scalar that can be used as an expression.
func (f Int8FieldSelector) Value(x int8) *Scalar {
	return &Scalar{
		Type: field.Int8,
		Data: field.EncodeInt8(x),
	}
}

// Int8Value is an expression that evaluates to itself.
type Int8Value int8

// Eval implements the Expr interface. It returns a scalar after encoding v to
// the right type.
func (v Int8Value) Eval(EvalContext) (Scalar, error) {
	return Scalar{
		Type: field.Int8,
		Data: field.EncodeInt8(int8(v)),
	}, nil
}

// EqInt16 matches if x is equal to the field selected by f.
func EqInt16(f FieldSelector, x int16) Expr {
	return &eqMatcher{
		Field: f,
		Value: field.EncodeInt16(x),
	}
}

// GtInt16 matches if x is greater than the field selected by f.
func GtInt16(f FieldSelector, x int16) Expr {
	return &gtMatcher{
		Field: f,
		Value: field.EncodeInt16(x),
	}
}

// GteInt16 matches if x is greater than or equal to the field selected by f.
func GteInt16(f FieldSelector, x int16) Expr {
	return &gteMatcher{
		Field: f,
		Value: field.EncodeInt16(x),
	}
}

// LtInt16 matches if x is less than the field selected by f.
func LtInt16(f FieldSelector, x int16) Expr {
	return &ltMatcher{
		Field: f,
		Value: field.EncodeInt16(x),
	}
}

// LteInt16 matches if x is less than or equal to the field selected by f.
func LteInt16(f FieldSelector, x int16) Expr {
	return &lteMatcher{
		Field: f,
		Value: field.EncodeInt16(x),
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
	return EqInt16(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f Int16FieldSelector) Gt(x int16) Expr {
	return GtInt16(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Int16FieldSelector) Gte(x int16) Expr {
	return GteInt16(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f Int16FieldSelector) Lt(x int16) Expr {
	return LtInt16(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Int16FieldSelector) Lte(x int16) Expr {
	return LteInt16(f.FieldSelector, x)
}

// Value returns a scalar that can be used as an expression.
func (f Int16FieldSelector) Value(x int16) *Scalar {
	return &Scalar{
		Type: field.Int16,
		Data: field.EncodeInt16(x),
	}
}

// Int16Value is an expression that evaluates to itself.
type Int16Value int16

// Eval implements the Expr interface. It returns a scalar after encoding v to
// the right type.
func (v Int16Value) Eval(EvalContext) (Scalar, error) {
	return Scalar{
		Type: field.Int16,
		Data: field.EncodeInt16(int16(v)),
	}, nil
}

// EqInt32 matches if x is equal to the field selected by f.
func EqInt32(f FieldSelector, x int32) Expr {
	return &eqMatcher{
		Field: f,
		Value: field.EncodeInt32(x),
	}
}

// GtInt32 matches if x is greater than the field selected by f.
func GtInt32(f FieldSelector, x int32) Expr {
	return &gtMatcher{
		Field: f,
		Value: field.EncodeInt32(x),
	}
}

// GteInt32 matches if x is greater than or equal to the field selected by f.
func GteInt32(f FieldSelector, x int32) Expr {
	return &gteMatcher{
		Field: f,
		Value: field.EncodeInt32(x),
	}
}

// LtInt32 matches if x is less than the field selected by f.
func LtInt32(f FieldSelector, x int32) Expr {
	return &ltMatcher{
		Field: f,
		Value: field.EncodeInt32(x),
	}
}

// LteInt32 matches if x is less than or equal to the field selected by f.
func LteInt32(f FieldSelector, x int32) Expr {
	return &lteMatcher{
		Field: f,
		Value: field.EncodeInt32(x),
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
	return EqInt32(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f Int32FieldSelector) Gt(x int32) Expr {
	return GtInt32(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Int32FieldSelector) Gte(x int32) Expr {
	return GteInt32(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f Int32FieldSelector) Lt(x int32) Expr {
	return LtInt32(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Int32FieldSelector) Lte(x int32) Expr {
	return LteInt32(f.FieldSelector, x)
}

// Value returns a scalar that can be used as an expression.
func (f Int32FieldSelector) Value(x int32) *Scalar {
	return &Scalar{
		Type: field.Int32,
		Data: field.EncodeInt32(x),
	}
}

// Int32Value is an expression that evaluates to itself.
type Int32Value int32

// Eval implements the Expr interface. It returns a scalar after encoding v to
// the right type.
func (v Int32Value) Eval(EvalContext) (Scalar, error) {
	return Scalar{
		Type: field.Int32,
		Data: field.EncodeInt32(int32(v)),
	}, nil
}

// EqInt64 matches if x is equal to the field selected by f.
func EqInt64(f FieldSelector, x int64) Expr {
	return &eqMatcher{
		Field: f,
		Value: field.EncodeInt64(x),
	}
}

// GtInt64 matches if x is greater than the field selected by f.
func GtInt64(f FieldSelector, x int64) Expr {
	return &gtMatcher{
		Field: f,
		Value: field.EncodeInt64(x),
	}
}

// GteInt64 matches if x is greater than or equal to the field selected by f.
func GteInt64(f FieldSelector, x int64) Expr {
	return &gteMatcher{
		Field: f,
		Value: field.EncodeInt64(x),
	}
}

// LtInt64 matches if x is less than the field selected by f.
func LtInt64(f FieldSelector, x int64) Expr {
	return &ltMatcher{
		Field: f,
		Value: field.EncodeInt64(x),
	}
}

// LteInt64 matches if x is less than or equal to the field selected by f.
func LteInt64(f FieldSelector, x int64) Expr {
	return &lteMatcher{
		Field: f,
		Value: field.EncodeInt64(x),
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
	return EqInt64(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f Int64FieldSelector) Gt(x int64) Expr {
	return GtInt64(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Int64FieldSelector) Gte(x int64) Expr {
	return GteInt64(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f Int64FieldSelector) Lt(x int64) Expr {
	return LtInt64(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Int64FieldSelector) Lte(x int64) Expr {
	return LteInt64(f.FieldSelector, x)
}

// Value returns a scalar that can be used as an expression.
func (f Int64FieldSelector) Value(x int64) *Scalar {
	return &Scalar{
		Type: field.Int64,
		Data: field.EncodeInt64(x),
	}
}

// Int64Value is an expression that evaluates to itself.
type Int64Value int64

// Eval implements the Expr interface. It returns a scalar after encoding v to
// the right type.
func (v Int64Value) Eval(EvalContext) (Scalar, error) {
	return Scalar{
		Type: field.Int64,
		Data: field.EncodeInt64(int64(v)),
	}, nil
}

// EqFloat32 matches if x is equal to the field selected by f.
func EqFloat32(f FieldSelector, x float32) Expr {
	return &eqMatcher{
		Field: f,
		Value: field.EncodeFloat32(x),
	}
}

// GtFloat32 matches if x is greater than the field selected by f.
func GtFloat32(f FieldSelector, x float32) Expr {
	return &gtMatcher{
		Field: f,
		Value: field.EncodeFloat32(x),
	}
}

// GteFloat32 matches if x is greater than or equal to the field selected by f.
func GteFloat32(f FieldSelector, x float32) Expr {
	return &gteMatcher{
		Field: f,
		Value: field.EncodeFloat32(x),
	}
}

// LtFloat32 matches if x is less than the field selected by f.
func LtFloat32(f FieldSelector, x float32) Expr {
	return &ltMatcher{
		Field: f,
		Value: field.EncodeFloat32(x),
	}
}

// LteFloat32 matches if x is less than or equal to the field selected by f.
func LteFloat32(f FieldSelector, x float32) Expr {
	return &lteMatcher{
		Field: f,
		Value: field.EncodeFloat32(x),
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
	return EqFloat32(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f Float32FieldSelector) Gt(x float32) Expr {
	return GtFloat32(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Float32FieldSelector) Gte(x float32) Expr {
	return GteFloat32(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f Float32FieldSelector) Lt(x float32) Expr {
	return LtFloat32(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Float32FieldSelector) Lte(x float32) Expr {
	return LteFloat32(f.FieldSelector, x)
}

// Value returns a scalar that can be used as an expression.
func (f Float32FieldSelector) Value(x float32) *Scalar {
	return &Scalar{
		Type: field.Float32,
		Data: field.EncodeFloat32(x),
	}
}

// Float32Value is an expression that evaluates to itself.
type Float32Value float32

// Eval implements the Expr interface. It returns a scalar after encoding v to
// the right type.
func (v Float32Value) Eval(EvalContext) (Scalar, error) {
	return Scalar{
		Type: field.Float32,
		Data: field.EncodeFloat32(float32(v)),
	}, nil
}

// EqFloat64 matches if x is equal to the field selected by f.
func EqFloat64(f FieldSelector, x float64) Expr {
	return &eqMatcher{
		Field: f,
		Value: field.EncodeFloat64(x),
	}
}

// GtFloat64 matches if x is greater than the field selected by f.
func GtFloat64(f FieldSelector, x float64) Expr {
	return &gtMatcher{
		Field: f,
		Value: field.EncodeFloat64(x),
	}
}

// GteFloat64 matches if x is greater than or equal to the field selected by f.
func GteFloat64(f FieldSelector, x float64) Expr {
	return &gteMatcher{
		Field: f,
		Value: field.EncodeFloat64(x),
	}
}

// LtFloat64 matches if x is less than the field selected by f.
func LtFloat64(f FieldSelector, x float64) Expr {
	return &ltMatcher{
		Field: f,
		Value: field.EncodeFloat64(x),
	}
}

// LteFloat64 matches if x is less than or equal to the field selected by f.
func LteFloat64(f FieldSelector, x float64) Expr {
	return &lteMatcher{
		Field: f,
		Value: field.EncodeFloat64(x),
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
	return EqFloat64(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f Float64FieldSelector) Gt(x float64) Expr {
	return GtFloat64(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Float64FieldSelector) Gte(x float64) Expr {
	return GteFloat64(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f Float64FieldSelector) Lt(x float64) Expr {
	return LtFloat64(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Float64FieldSelector) Lte(x float64) Expr {
	return LteFloat64(f.FieldSelector, x)
}

// Value returns a scalar that can be used as an expression.
func (f Float64FieldSelector) Value(x float64) *Scalar {
	return &Scalar{
		Type: field.Float64,
		Data: field.EncodeFloat64(x),
	}
}

// Float64Value is an expression that evaluates to itself.
type Float64Value float64

// Eval implements the Expr interface. It returns a scalar after encoding v to
// the right type.
func (v Float64Value) Eval(EvalContext) (Scalar, error) {
	return Scalar{
		Type: field.Float64,
		Data: field.EncodeFloat64(float64(v)),
	}, nil
}


