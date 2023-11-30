package encoding

// Types used to encode values in the tree.
// They are sorted from the smallest to largest.
// Each type is encoded on 1 byte and describes two things:
// - the type of the field
// - the sort order of the field (ASC or DESC)
// The first 128 values are used for ASC order, the last 128 values are used for DESC order.
// A block of 64 is reserved for small integers in the range [-32, 31].
// Gaps are left between each type to allow adding new types in the future.
const (
	TombstoneValue byte = 0

	// 1: 1 type is free

	// Null
	NullValue byte = 2

	// 3, 4: 2 types are free

	// Booleans
	FalseValue byte = 5
	TrueValue  byte = 6

	// 7 to 11: 5 types are free

	// Negative integers
	Int64Value byte = 12
	Int32Value byte = 13
	Int16Value byte = 14
	Int8Value  byte = 15

	// Contiguous block of 64 integers.
	// Types from 16 to 79 represent
	// values from -32 to 31
	IntSmallValue byte = 16

	// Positive integers
	Uint8Value  byte = 80
	Uint16Value byte = 81
	Uint32Value byte = 82
	Uint64Value byte = 83

	// 84 to 89: 6 types are free

	// Floating point numbers
	Float64Value byte = 90

	// 92 to 97: 6 types are free

	// Text
	TextValue byte = 98

	// 101 to 105: 5 types are free

	// Binary
	BlobValue byte = 103

	// 104 to 109: 6 types are free

	// Arrays
	ArrayValue byte = 110

	// 111 to 119: 9 types are free

	// Objects
	ObjectValue byte = 120

	// 121 to 127: 7 types are free

	// The second half of the byte is organized in reverse order, and it
	// symmetrical to the first 128 values.

	// DESC_ prefix means that the value is encoded in reverse order.
	DESC_ObjectValue   byte = 255 - ObjectValue
	DESC_ArrayValue    byte = 255 - ArrayValue
	DESC_BlobValue     byte = 255 - BlobValue
	DESC_TextValue     byte = 255 - TextValue
	DESC_Float64Value  byte = 255 - Float64Value
	DESC_Uint64Value   byte = 255 - Uint64Value
	DESC_Uint32Value   byte = 255 - Uint32Value
	DESC_Uint16Value   byte = 255 - Uint16Value
	DESC_Uint8Value    byte = 255 - Uint8Value
	DESC_IntSmallValue byte = 255 - IntSmallValue
	DESC_Int8Value     byte = 255 - Int8Value
	DESC_Int16Value    byte = 255 - Int16Value
	DESC_Int32Value    byte = 255 - Int32Value
	DESC_Int64Value    byte = 255 - Int64Value
	DESC_TrueValue     byte = 255 - TrueValue
	DESC_FalseValue    byte = 255 - FalseValue
	DESC_NullValue     byte = 255 - NullValue
)
