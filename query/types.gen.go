
package query

import (
	"github.com/asdine/genji/field"
)

// EqBytes matches if x is equal to the field selected by f.
func EqBytes(f FieldSelector, x []byte) *EqMatcher {
	return &EqMatcher{
		f: f,
		v: field.EncodeBytes(x),
	}
}

// GtBytes matches if x is greater than the field selected by f.
func GtBytes(f FieldSelector, x []byte) *GtMatcher {
	return &GtMatcher{
		f: f,
		v: field.EncodeBytes(x),
	}
}

// GteBytes matches if x is greater than or equal to the field selected by f.
func GteBytes(f FieldSelector, x []byte) *GteMatcher {
	return &GteMatcher{
		f: f,
		v: field.EncodeBytes(x),
	}
}

// LtBytes matches if x is less than the field selected by f.
func LtBytes(f FieldSelector, x []byte) *LtMatcher {
	return &LtMatcher{
		f: f,
		v: field.EncodeBytes(x),
	}
}

// LteBytes matches if x is less than or equal to the field selected by f.
func LteBytes(f FieldSelector, x []byte) *LteMatcher {
	return &LteMatcher{
		f: f,
		v: field.EncodeBytes(x),
	}
}

// BytesField is a type safe selector that allows to compare values with fields
// based on their types.
type BytesField struct {
	FieldSelector
}

// NewBytesField creates a typed FieldSelector for fields of type []byte.
func NewBytesField(name string) BytesField {
	return BytesField{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f BytesField) Eq(x []byte) Matcher {
	return EqBytes(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f BytesField) Gt(x []byte) Matcher {
	return GtBytes(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f BytesField) Gte(x []byte) Matcher {
	return GteBytes(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f BytesField) Lt(x []byte) Matcher {
	return LtBytes(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f BytesField) Lte(x []byte) Matcher {
	return LteBytes(f.FieldSelector, x)
}

// EqString matches if x is equal to the field selected by f.
func EqString(f FieldSelector, x string) *EqMatcher {
	return &EqMatcher{
		f: f,
		v: field.EncodeString(x),
	}
}

// GtString matches if x is greater than the field selected by f.
func GtString(f FieldSelector, x string) *GtMatcher {
	return &GtMatcher{
		f: f,
		v: field.EncodeString(x),
	}
}

// GteString matches if x is greater than or equal to the field selected by f.
func GteString(f FieldSelector, x string) *GteMatcher {
	return &GteMatcher{
		f: f,
		v: field.EncodeString(x),
	}
}

// LtString matches if x is less than the field selected by f.
func LtString(f FieldSelector, x string) *LtMatcher {
	return &LtMatcher{
		f: f,
		v: field.EncodeString(x),
	}
}

// LteString matches if x is less than or equal to the field selected by f.
func LteString(f FieldSelector, x string) *LteMatcher {
	return &LteMatcher{
		f: f,
		v: field.EncodeString(x),
	}
}


// StringField is a type safe selector that allows to compare values with fields
// based on their types.
type StringField struct {
	FieldSelector
}

// NewStringField creates a typed FieldSelector for fields of type string.
func NewStringField(name string) StringField {
	return StringField{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f StringField) Eq(x string) Matcher {
	return EqString(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f StringField) Gt(x string) Matcher {
	return GtString(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f StringField) Gte(x string) Matcher {
	return GteString(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f StringField) Lt(x string) Matcher {
	return LtString(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f StringField) Lte(x string) Matcher {
	return LteString(f.FieldSelector, x)
}

// EqBool matches if x is equal to the field selected by f.
func EqBool(f FieldSelector, x bool) *EqMatcher {
	return &EqMatcher{
		f: f,
		v: field.EncodeBool(x),
	}
}

// GtBool matches if x is greater than the field selected by f.
func GtBool(f FieldSelector, x bool) *GtMatcher {
	return &GtMatcher{
		f: f,
		v: field.EncodeBool(x),
	}
}

// GteBool matches if x is greater than or equal to the field selected by f.
func GteBool(f FieldSelector, x bool) *GteMatcher {
	return &GteMatcher{
		f: f,
		v: field.EncodeBool(x),
	}
}

// LtBool matches if x is less than the field selected by f.
func LtBool(f FieldSelector, x bool) *LtMatcher {
	return &LtMatcher{
		f: f,
		v: field.EncodeBool(x),
	}
}

// LteBool matches if x is less than or equal to the field selected by f.
func LteBool(f FieldSelector, x bool) *LteMatcher {
	return &LteMatcher{
		f: f,
		v: field.EncodeBool(x),
	}
}

// BoolField is a type safe selector that allows to compare values with fields
// based on their types.
type BoolField struct {
	FieldSelector
}

// NewBoolField creates a typed FieldSelector for fields of type bool.
func NewBoolField(name string) BoolField {
	return BoolField{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f BoolField) Eq(x bool) Matcher {
	return EqBool(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f BoolField) Gt(x bool) Matcher {
	return GtBool(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f BoolField) Gte(x bool) Matcher {
	return GteBool(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f BoolField) Lt(x bool) Matcher {
	return LtBool(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f BoolField) Lte(x bool) Matcher {
	return LteBool(f.FieldSelector, x)
}

// EqUint matches if x is equal to the field selected by f.
func EqUint(f FieldSelector, x uint) *EqMatcher {
	return &EqMatcher{
		f: f,
		v: field.EncodeUint(x),
	}
}

// GtUint matches if x is greater than the field selected by f.
func GtUint(f FieldSelector, x uint) *GtMatcher {
	return &GtMatcher{
		f: f,
		v: field.EncodeUint(x),
	}
}

// GteUint matches if x is greater than or equal to the field selected by f.
func GteUint(f FieldSelector, x uint) *GteMatcher {
	return &GteMatcher{
		f: f,
		v: field.EncodeUint(x),
	}
}

// LtUint matches if x is less than the field selected by f.
func LtUint(f FieldSelector, x uint) *LtMatcher {
	return &LtMatcher{
		f: f,
		v: field.EncodeUint(x),
	}
}

// LteUint matches if x is less than or equal to the field selected by f.
func LteUint(f FieldSelector, x uint) *LteMatcher {
	return &LteMatcher{
		f: f,
		v: field.EncodeUint(x),
	}
}

// UintField is a type safe selector that allows to compare values with fields
// based on their types.
type UintField struct {
	FieldSelector
}

// NewUintField creates a typed FieldSelector for fields of type uint.
func NewUintField(name string) UintField {
	return UintField{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f UintField) Eq(x uint) Matcher {
	return EqUint(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f UintField) Gt(x uint) Matcher {
	return GtUint(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f UintField) Gte(x uint) Matcher {
	return GteUint(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f UintField) Lt(x uint) Matcher {
	return LtUint(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f UintField) Lte(x uint) Matcher {
	return LteUint(f.FieldSelector, x)
}

// EqUint8 matches if x is equal to the field selected by f.
func EqUint8(f FieldSelector, x uint8) *EqMatcher {
	return &EqMatcher{
		f: f,
		v: field.EncodeUint8(x),
	}
}

// GtUint8 matches if x is greater than the field selected by f.
func GtUint8(f FieldSelector, x uint8) *GtMatcher {
	return &GtMatcher{
		f: f,
		v: field.EncodeUint8(x),
	}
}

// GteUint8 matches if x is greater than or equal to the field selected by f.
func GteUint8(f FieldSelector, x uint8) *GteMatcher {
	return &GteMatcher{
		f: f,
		v: field.EncodeUint8(x),
	}
}

// LtUint8 matches if x is less than the field selected by f.
func LtUint8(f FieldSelector, x uint8) *LtMatcher {
	return &LtMatcher{
		f: f,
		v: field.EncodeUint8(x),
	}
}

// LteUint8 matches if x is less than or equal to the field selected by f.
func LteUint8(f FieldSelector, x uint8) *LteMatcher {
	return &LteMatcher{
		f: f,
		v: field.EncodeUint8(x),
	}
}

// Uint8Field is a type safe selector that allows to compare values with fields
// based on their types.
type Uint8Field struct {
	FieldSelector
}

// NewUint8Field creates a typed FieldSelector for fields of type uint8.
func NewUint8Field(name string) Uint8Field {
	return Uint8Field{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Uint8Field) Eq(x uint8) Matcher {
	return EqUint8(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f Uint8Field) Gt(x uint8) Matcher {
	return GtUint8(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Uint8Field) Gte(x uint8) Matcher {
	return GteUint8(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f Uint8Field) Lt(x uint8) Matcher {
	return LtUint8(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Uint8Field) Lte(x uint8) Matcher {
	return LteUint8(f.FieldSelector, x)
}

// EqUint16 matches if x is equal to the field selected by f.
func EqUint16(f FieldSelector, x uint16) *EqMatcher {
	return &EqMatcher{
		f: f,
		v: field.EncodeUint16(x),
	}
}

// GtUint16 matches if x is greater than the field selected by f.
func GtUint16(f FieldSelector, x uint16) *GtMatcher {
	return &GtMatcher{
		f: f,
		v: field.EncodeUint16(x),
	}
}

// GteUint16 matches if x is greater than or equal to the field selected by f.
func GteUint16(f FieldSelector, x uint16) *GteMatcher {
	return &GteMatcher{
		f: f,
		v: field.EncodeUint16(x),
	}
}

// LtUint16 matches if x is less than the field selected by f.
func LtUint16(f FieldSelector, x uint16) *LtMatcher {
	return &LtMatcher{
		f: f,
		v: field.EncodeUint16(x),
	}
}

// LteUint16 matches if x is less than or equal to the field selected by f.
func LteUint16(f FieldSelector, x uint16) *LteMatcher {
	return &LteMatcher{
		f: f,
		v: field.EncodeUint16(x),
	}
}

// Uint16Field is a type safe selector that allows to compare values with fields
// based on their types.
type Uint16Field struct {
	FieldSelector
}

// NewUint16Field creates a typed FieldSelector for fields of type uint16.
func NewUint16Field(name string) Uint16Field {
	return Uint16Field{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Uint16Field) Eq(x uint16) Matcher {
	return EqUint16(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f Uint16Field) Gt(x uint16) Matcher {
	return GtUint16(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Uint16Field) Gte(x uint16) Matcher {
	return GteUint16(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f Uint16Field) Lt(x uint16) Matcher {
	return LtUint16(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Uint16Field) Lte(x uint16) Matcher {
	return LteUint16(f.FieldSelector, x)
}

// EqUint32 matches if x is equal to the field selected by f.
func EqUint32(f FieldSelector, x uint32) *EqMatcher {
	return &EqMatcher{
		f: f,
		v: field.EncodeUint32(x),
	}
}

// GtUint32 matches if x is greater than the field selected by f.
func GtUint32(f FieldSelector, x uint32) *GtMatcher {
	return &GtMatcher{
		f: f,
		v: field.EncodeUint32(x),
	}
}

// GteUint32 matches if x is greater than or equal to the field selected by f.
func GteUint32(f FieldSelector, x uint32) *GteMatcher {
	return &GteMatcher{
		f: f,
		v: field.EncodeUint32(x),
	}
}

// LtUint32 matches if x is less than the field selected by f.
func LtUint32(f FieldSelector, x uint32) *LtMatcher {
	return &LtMatcher{
		f: f,
		v: field.EncodeUint32(x),
	}
}

// LteUint32 matches if x is less than or equal to the field selected by f.
func LteUint32(f FieldSelector, x uint32) *LteMatcher {
	return &LteMatcher{
		f: f,
		v: field.EncodeUint32(x),
	}
}

// Uint32Field is a type safe selector that allows to compare values with fields
// based on their types.
type Uint32Field struct {
	FieldSelector
}

// NewUint32Field creates a typed FieldSelector for fields of type uint32.
func NewUint32Field(name string) Uint32Field {
	return Uint32Field{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Uint32Field) Eq(x uint32) Matcher {
	return EqUint32(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f Uint32Field) Gt(x uint32) Matcher {
	return GtUint32(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Uint32Field) Gte(x uint32) Matcher {
	return GteUint32(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f Uint32Field) Lt(x uint32) Matcher {
	return LtUint32(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Uint32Field) Lte(x uint32) Matcher {
	return LteUint32(f.FieldSelector, x)
}

// EqUint64 matches if x is equal to the field selected by f.
func EqUint64(f FieldSelector, x uint64) *EqMatcher {
	return &EqMatcher{
		f: f,
		v: field.EncodeUint64(x),
	}
}

// GtUint64 matches if x is greater than the field selected by f.
func GtUint64(f FieldSelector, x uint64) *GtMatcher {
	return &GtMatcher{
		f: f,
		v: field.EncodeUint64(x),
	}
}

// GteUint64 matches if x is greater than or equal to the field selected by f.
func GteUint64(f FieldSelector, x uint64) *GteMatcher {
	return &GteMatcher{
		f: f,
		v: field.EncodeUint64(x),
	}
}

// LtUint64 matches if x is less than the field selected by f.
func LtUint64(f FieldSelector, x uint64) *LtMatcher {
	return &LtMatcher{
		f: f,
		v: field.EncodeUint64(x),
	}
}

// LteUint64 matches if x is less than or equal to the field selected by f.
func LteUint64(f FieldSelector, x uint64) *LteMatcher {
	return &LteMatcher{
		f: f,
		v: field.EncodeUint64(x),
	}
}

// Uint64Field is a type safe selector that allows to compare values with fields
// based on their types.
type Uint64Field struct {
	FieldSelector
}

// NewUint64Field creates a typed FieldSelector for fields of type uint64.
func NewUint64Field(name string) Uint64Field {
	return Uint64Field{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Uint64Field) Eq(x uint64) Matcher {
	return EqUint64(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f Uint64Field) Gt(x uint64) Matcher {
	return GtUint64(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Uint64Field) Gte(x uint64) Matcher {
	return GteUint64(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f Uint64Field) Lt(x uint64) Matcher {
	return LtUint64(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Uint64Field) Lte(x uint64) Matcher {
	return LteUint64(f.FieldSelector, x)
}

// EqInt matches if x is equal to the field selected by f.
func EqInt(f FieldSelector, x int) *EqMatcher {
	return &EqMatcher{
		f: f,
		v: field.EncodeInt(x),
	}
}

// GtInt matches if x is greater than the field selected by f.
func GtInt(f FieldSelector, x int) *GtMatcher {
	return &GtMatcher{
		f: f,
		v: field.EncodeInt(x),
	}
}

// GteInt matches if x is greater than or equal to the field selected by f.
func GteInt(f FieldSelector, x int) *GteMatcher {
	return &GteMatcher{
		f: f,
		v: field.EncodeInt(x),
	}
}

// LtInt matches if x is less than the field selected by f.
func LtInt(f FieldSelector, x int) *LtMatcher {
	return &LtMatcher{
		f: f,
		v: field.EncodeInt(x),
	}
}

// LteInt matches if x is less than or equal to the field selected by f.
func LteInt(f FieldSelector, x int) *LteMatcher {
	return &LteMatcher{
		f: f,
		v: field.EncodeInt(x),
	}
}

// IntField is a type safe selector that allows to compare values with fields
// based on their types.
type IntField struct {
	FieldSelector
}

// NewIntField creates a typed FieldSelector for fields of type int.
func NewIntField(name string) IntField {
	return IntField{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f IntField) Eq(x int) Matcher {
	return EqInt(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f IntField) Gt(x int) Matcher {
	return GtInt(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f IntField) Gte(x int) Matcher {
	return GteInt(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f IntField) Lt(x int) Matcher {
	return LtInt(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f IntField) Lte(x int) Matcher {
	return LteInt(f.FieldSelector, x)
}

// EqInt8 matches if x is equal to the field selected by f.
func EqInt8(f FieldSelector, x int8) *EqMatcher {
	return &EqMatcher{
		f: f,
		v: field.EncodeInt8(x),
	}
}

// GtInt8 matches if x is greater than the field selected by f.
func GtInt8(f FieldSelector, x int8) *GtMatcher {
	return &GtMatcher{
		f: f,
		v: field.EncodeInt8(x),
	}
}

// GteInt8 matches if x is greater than or equal to the field selected by f.
func GteInt8(f FieldSelector, x int8) *GteMatcher {
	return &GteMatcher{
		f: f,
		v: field.EncodeInt8(x),
	}
}

// LtInt8 matches if x is less than the field selected by f.
func LtInt8(f FieldSelector, x int8) *LtMatcher {
	return &LtMatcher{
		f: f,
		v: field.EncodeInt8(x),
	}
}

// LteInt8 matches if x is less than or equal to the field selected by f.
func LteInt8(f FieldSelector, x int8) *LteMatcher {
	return &LteMatcher{
		f: f,
		v: field.EncodeInt8(x),
	}
}

// Int8Field is a type safe selector that allows to compare values with fields
// based on their types.
type Int8Field struct {
	FieldSelector
}

// NewInt8Field creates a typed FieldSelector for fields of type int8.
func NewInt8Field(name string) Int8Field {
	return Int8Field{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Int8Field) Eq(x int8) Matcher {
	return EqInt8(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f Int8Field) Gt(x int8) Matcher {
	return GtInt8(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Int8Field) Gte(x int8) Matcher {
	return GteInt8(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f Int8Field) Lt(x int8) Matcher {
	return LtInt8(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Int8Field) Lte(x int8) Matcher {
	return LteInt8(f.FieldSelector, x)
}

// EqInt16 matches if x is equal to the field selected by f.
func EqInt16(f FieldSelector, x int16) *EqMatcher {
	return &EqMatcher{
		f: f,
		v: field.EncodeInt16(x),
	}
}

// GtInt16 matches if x is greater than the field selected by f.
func GtInt16(f FieldSelector, x int16) *GtMatcher {
	return &GtMatcher{
		f: f,
		v: field.EncodeInt16(x),
	}
}

// GteInt16 matches if x is greater than or equal to the field selected by f.
func GteInt16(f FieldSelector, x int16) *GteMatcher {
	return &GteMatcher{
		f: f,
		v: field.EncodeInt16(x),
	}
}

// LtInt16 matches if x is less than the field selected by f.
func LtInt16(f FieldSelector, x int16) *LtMatcher {
	return &LtMatcher{
		f: f,
		v: field.EncodeInt16(x),
	}
}

// LteInt16 matches if x is less than or equal to the field selected by f.
func LteInt16(f FieldSelector, x int16) *LteMatcher {
	return &LteMatcher{
		f: f,
		v: field.EncodeInt16(x),
	}
}

// Int16Field is a type safe selector that allows to compare values with fields
// based on their types.
type Int16Field struct {
	FieldSelector
}

// NewInt16Field creates a typed FieldSelector for fields of type int16.
func NewInt16Field(name string) Int16Field {
	return Int16Field{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Int16Field) Eq(x int16) Matcher {
	return EqInt16(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f Int16Field) Gt(x int16) Matcher {
	return GtInt16(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Int16Field) Gte(x int16) Matcher {
	return GteInt16(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f Int16Field) Lt(x int16) Matcher {
	return LtInt16(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Int16Field) Lte(x int16) Matcher {
	return LteInt16(f.FieldSelector, x)
}

// EqInt32 matches if x is equal to the field selected by f.
func EqInt32(f FieldSelector, x int32) *EqMatcher {
	return &EqMatcher{
		f: f,
		v: field.EncodeInt32(x),
	}
}

// GtInt32 matches if x is greater than the field selected by f.
func GtInt32(f FieldSelector, x int32) *GtMatcher {
	return &GtMatcher{
		f: f,
		v: field.EncodeInt32(x),
	}
}

// GteInt32 matches if x is greater than or equal to the field selected by f.
func GteInt32(f FieldSelector, x int32) *GteMatcher {
	return &GteMatcher{
		f: f,
		v: field.EncodeInt32(x),
	}
}

// LtInt32 matches if x is less than the field selected by f.
func LtInt32(f FieldSelector, x int32) *LtMatcher {
	return &LtMatcher{
		f: f,
		v: field.EncodeInt32(x),
	}
}

// LteInt32 matches if x is less than or equal to the field selected by f.
func LteInt32(f FieldSelector, x int32) *LteMatcher {
	return &LteMatcher{
		f: f,
		v: field.EncodeInt32(x),
	}
}

// Int32Field is a type safe selector that allows to compare values with fields
// based on their types.
type Int32Field struct {
	FieldSelector
}

// NewInt32Field creates a typed FieldSelector for fields of type int32.
func NewInt32Field(name string) Int32Field {
	return Int32Field{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Int32Field) Eq(x int32) Matcher {
	return EqInt32(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f Int32Field) Gt(x int32) Matcher {
	return GtInt32(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Int32Field) Gte(x int32) Matcher {
	return GteInt32(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f Int32Field) Lt(x int32) Matcher {
	return LtInt32(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Int32Field) Lte(x int32) Matcher {
	return LteInt32(f.FieldSelector, x)
}

// EqInt64 matches if x is equal to the field selected by f.
func EqInt64(f FieldSelector, x int64) *EqMatcher {
	return &EqMatcher{
		f: f,
		v: field.EncodeInt64(x),
	}
}

// GtInt64 matches if x is greater than the field selected by f.
func GtInt64(f FieldSelector, x int64) *GtMatcher {
	return &GtMatcher{
		f: f,
		v: field.EncodeInt64(x),
	}
}

// GteInt64 matches if x is greater than or equal to the field selected by f.
func GteInt64(f FieldSelector, x int64) *GteMatcher {
	return &GteMatcher{
		f: f,
		v: field.EncodeInt64(x),
	}
}

// LtInt64 matches if x is less than the field selected by f.
func LtInt64(f FieldSelector, x int64) *LtMatcher {
	return &LtMatcher{
		f: f,
		v: field.EncodeInt64(x),
	}
}

// LteInt64 matches if x is less than or equal to the field selected by f.
func LteInt64(f FieldSelector, x int64) *LteMatcher {
	return &LteMatcher{
		f: f,
		v: field.EncodeInt64(x),
	}
}

// Int64Field is a type safe selector that allows to compare values with fields
// based on their types.
type Int64Field struct {
	FieldSelector
}

// NewInt64Field creates a typed FieldSelector for fields of type int64.
func NewInt64Field(name string) Int64Field {
	return Int64Field{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Int64Field) Eq(x int64) Matcher {
	return EqInt64(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f Int64Field) Gt(x int64) Matcher {
	return GtInt64(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Int64Field) Gte(x int64) Matcher {
	return GteInt64(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f Int64Field) Lt(x int64) Matcher {
	return LtInt64(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Int64Field) Lte(x int64) Matcher {
	return LteInt64(f.FieldSelector, x)
}

// EqFloat32 matches if x is equal to the field selected by f.
func EqFloat32(f FieldSelector, x float32) *EqMatcher {
	return &EqMatcher{
		f: f,
		v: field.EncodeFloat32(x),
	}
}

// GtFloat32 matches if x is greater than the field selected by f.
func GtFloat32(f FieldSelector, x float32) *GtMatcher {
	return &GtMatcher{
		f: f,
		v: field.EncodeFloat32(x),
	}
}

// GteFloat32 matches if x is greater than or equal to the field selected by f.
func GteFloat32(f FieldSelector, x float32) *GteMatcher {
	return &GteMatcher{
		f: f,
		v: field.EncodeFloat32(x),
	}
}

// LtFloat32 matches if x is less than the field selected by f.
func LtFloat32(f FieldSelector, x float32) *LtMatcher {
	return &LtMatcher{
		f: f,
		v: field.EncodeFloat32(x),
	}
}

// LteFloat32 matches if x is less than or equal to the field selected by f.
func LteFloat32(f FieldSelector, x float32) *LteMatcher {
	return &LteMatcher{
		f: f,
		v: field.EncodeFloat32(x),
	}
}

// Float32Field is a type safe selector that allows to compare values with fields
// based on their types.
type Float32Field struct {
	FieldSelector
}

// NewFloat32Field creates a typed FieldSelector for fields of type float32.
func NewFloat32Field(name string) Float32Field {
	return Float32Field{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Float32Field) Eq(x float32) Matcher {
	return EqFloat32(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f Float32Field) Gt(x float32) Matcher {
	return GtFloat32(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Float32Field) Gte(x float32) Matcher {
	return GteFloat32(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f Float32Field) Lt(x float32) Matcher {
	return LtFloat32(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Float32Field) Lte(x float32) Matcher {
	return LteFloat32(f.FieldSelector, x)
}

// EqFloat64 matches if x is equal to the field selected by f.
func EqFloat64(f FieldSelector, x float64) *EqMatcher {
	return &EqMatcher{
		f: f,
		v: field.EncodeFloat64(x),
	}
}

// GtFloat64 matches if x is greater than the field selected by f.
func GtFloat64(f FieldSelector, x float64) *GtMatcher {
	return &GtMatcher{
		f: f,
		v: field.EncodeFloat64(x),
	}
}

// GteFloat64 matches if x is greater than or equal to the field selected by f.
func GteFloat64(f FieldSelector, x float64) *GteMatcher {
	return &GteMatcher{
		f: f,
		v: field.EncodeFloat64(x),
	}
}

// LtFloat64 matches if x is less than the field selected by f.
func LtFloat64(f FieldSelector, x float64) *LtMatcher {
	return &LtMatcher{
		f: f,
		v: field.EncodeFloat64(x),
	}
}

// LteFloat64 matches if x is less than or equal to the field selected by f.
func LteFloat64(f FieldSelector, x float64) *LteMatcher {
	return &LteMatcher{
		f: f,
		v: field.EncodeFloat64(x),
	}
}

// Float64Field is a type safe selector that allows to compare values with fields
// based on their types.
type Float64Field struct {
	FieldSelector
}

// NewFloat64Field creates a typed FieldSelector for fields of type float64.
func NewFloat64Field(name string) Float64Field {
	return Float64Field{FieldSelector: Field(name)}
}

// Eq matches if x is equal to the field selected by f.
func (f Float64Field) Eq(x float64) Matcher {
	return EqFloat64(f.FieldSelector, x)
}

// Gt matches if x is greater than the field selected by f.
func (f Float64Field) Gt(x float64) Matcher {
	return GtFloat64(f.FieldSelector, x)
}

// Gte matches if x is greater than or equal to the field selected by f.
func (f Float64Field) Gte(x float64) Matcher {
	return GteFloat64(f.FieldSelector, x)
}

// Lt matches if x is less than the field selected by f.
func (f Float64Field) Lt(x float64) Matcher {
	return LtFloat64(f.FieldSelector, x)
}

// Lte matches if x is less than or equal to the field selected by f.
func (f Float64Field) Lte(x float64) Matcher {
	return LteFloat64(f.FieldSelector, x)
}


