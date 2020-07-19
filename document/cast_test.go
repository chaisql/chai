package document

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCastAs(t *testing.T) {
	type test struct {
		v, want Value
		fails   bool
	}

	boolV := NewBoolValue(true)
	integerV := NewIntegerValue(10)
	durationV := NewDurationValue(3 * time.Second)
	doubleV := NewDoubleValue(10.5)
	textV := NewTextValue("foo")
	blobV := NewBlobValue([]byte("abc"))
	arrayV := NewArrayValue(NewValueBuffer().
		Append(NewTextValue("bar")).
		Append(integerV))
	docV := NewDocumentValue(NewFieldBuffer().
		Add("a", integerV).
		Add("b", textV))

	check := func(t *testing.T, targetType ValueType, tests []test) {
		for _, test := range tests {
			t.Run(test.v.String(), func(t *testing.T) {
				got, err := test.v.CastAs(targetType)
				if test.fails {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, test.want, got)
				}
			})
		}
	}

	t.Run("bool", func(t *testing.T) {
		check(t, BoolValue, []test{
			{boolV, boolV, false},
			{integerV, boolV, false},
			{NewIntegerValue(0), NewBoolValue(false), false},
			{durationV, Value{}, true},
			{doubleV, Value{}, true},
			{textV, Value{}, true},
			{NewTextValue("true"), boolV, false},
			{NewTextValue("false"), NewBoolValue(false), false},
			{blobV, Value{}, true},
			{arrayV, Value{}, true},
			{docV, Value{}, true},
		})
	})

	t.Run("integer", func(t *testing.T) {
		check(t, IntegerValue, []test{
			{boolV, NewIntegerValue(1), false},
			{NewBoolValue(false), NewIntegerValue(0), false},
			{integerV, integerV, false},
			{durationV, Value{}, true},
			{doubleV, integerV, false},
			{textV, Value{}, true},
			{NewTextValue("10"), integerV, false},
			{NewTextValue("10.5"), integerV, false},
			{blobV, Value{}, true},
			{arrayV, Value{}, true},
			{docV, Value{}, true},
		})
	})

	t.Run("double", func(t *testing.T) {
		check(t, DoubleValue, []test{
			{boolV, Value{}, true},
			{integerV, NewDoubleValue(10), false},
			{durationV, Value{}, true},
			{doubleV, doubleV, false},
			{textV, Value{}, true},
			{NewTextValue("10"), NewDoubleValue(10), false},
			{NewTextValue("10.5"), doubleV, false},
			{blobV, Value{}, true},
			{arrayV, Value{}, true},
			{docV, Value{}, true},
		})
	})

	t.Run("duration", func(t *testing.T) {
		check(t, DurationValue, []test{
			{boolV, Value{}, true},
			{integerV, Value{}, true},
			{durationV, durationV, false},
			{doubleV, Value{}, true},
			{textV, Value{}, true},
			{NewTextValue("3s"), durationV, false},
			{NewTextValue("10.5"), Value{}, true},
			{blobV, Value{}, true},
			{arrayV, Value{}, true},
			{docV, Value{}, true},
		})
	})

	t.Run("text", func(t *testing.T) {
		check(t, TextValue, []test{
			{boolV, NewTextValue("true"), false},
			{integerV, NewTextValue("10"), false},
			{durationV, NewTextValue("3s"), false},
			{doubleV, NewTextValue("10.5"), false},
			{textV, textV, false},
			{blobV, NewTextValue("YWJj"), false},
			{arrayV, NewTextValue(`["bar", 10]`), false},
			{docV,
				NewTextValue(`{"a": 10, "b": "foo"}`),
				false},
		})
	})

	t.Run("blob", func(t *testing.T) {
		check(t, BlobValue, []test{
			{boolV, Value{}, true},
			{integerV, Value{}, true},
			{durationV, Value{}, true},
			{doubleV, Value{}, true},
			{NewTextValue("YWJj"), blobV, false},
			{NewTextValue("   dww  "), Value{}, true},
			{blobV, blobV, false},
			{arrayV, Value{}, true},
			{docV, Value{}, true},
		})
	})

	t.Run("array", func(t *testing.T) {
		check(t, ArrayValue, []test{
			{boolV, Value{}, true},
			{integerV, Value{}, true},
			{durationV, Value{}, true},
			{doubleV, Value{}, true},
			{NewTextValue(`["bar", 10]`), arrayV, false},
			{NewTextValue("abc"), Value{}, true},
			{blobV, Value{}, true},
			{arrayV, arrayV, false},
			{docV, Value{}, true},
		})
	})

	t.Run("document", func(t *testing.T) {
		check(t, DocumentValue, []test{
			{boolV, Value{}, true},
			{integerV, Value{}, true},
			{durationV, Value{}, true},
			{doubleV, Value{}, true},
			{NewTextValue(`{"a": 10, "b": "foo"}`), docV, false},
			{NewTextValue("abc"), Value{}, true},
			{blobV, Value{}, true},
			{arrayV, Value{}, true},
			{docV, docV, false},
		})
	})
}
