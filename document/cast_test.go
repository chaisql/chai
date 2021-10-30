package document

import (
	"math"
	"testing"

	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func TestCastAs(t *testing.T) {
	type test struct {
		v, want types.Value
		fails   bool
	}

	boolV := types.NewBoolValue(true)
	integerV := types.NewIntegerValue(10)
	doubleV := types.NewDoubleValue(10.5)
	textV := types.NewTextValue("foo")
	blobV := types.NewBlobValue([]byte("asdine"))
	arrayV := types.NewArrayValue(NewValueBuffer().
		Append(types.NewTextValue("bar")).
		Append(integerV))
	docV := types.NewDocumentValue(NewFieldBuffer().
		Add("a", integerV).
		Add("b", textV))

	check := func(t *testing.T, targetType types.ValueType, tests []test) {
		t.Helper()

		for _, test := range tests {
			t.Run(test.v.String(), func(t *testing.T) {
				t.Helper()

				got, err := CastAs(test.v, targetType)
				if test.fails {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					require.Equal(t, test.want, got)
				}
			})
		}
	}

	t.Run("bool", func(t *testing.T) {
		check(t, types.BoolValue, []test{
			{boolV, boolV, false},
			{integerV, boolV, false},
			{types.NewIntegerValue(0), types.NewBoolValue(false), false},
			{doubleV, nil, true},
			{textV, nil, true},
			{types.NewTextValue("true"), boolV, false},
			{types.NewTextValue("false"), types.NewBoolValue(false), false},
			{blobV, nil, true},
			{arrayV, nil, true},
			{docV, nil, true},
		})
	})

	t.Run("integer", func(t *testing.T) {
		check(t, types.IntegerValue, []test{
			{boolV, types.NewIntegerValue(1), false},
			{types.NewBoolValue(false), types.NewIntegerValue(0), false},
			{integerV, integerV, false},
			{doubleV, integerV, false},
			{textV, nil, true},
			{types.NewTextValue("10"), integerV, false},
			{types.NewTextValue("10.5"), integerV, false},
			{blobV, nil, true},
			{arrayV, nil, true},
			{docV, nil, true},
			{types.NewDoubleValue(math.MaxInt64 + 1), nil, true},
		})
	})

	t.Run("double", func(t *testing.T) {
		check(t, types.DoubleValue, []test{
			{boolV, nil, true},
			{integerV, types.NewDoubleValue(10), false},
			{doubleV, doubleV, false},
			{textV, nil, true},
			{types.NewTextValue("10"), types.NewDoubleValue(10), false},
			{types.NewTextValue("10.5"), doubleV, false},
			{blobV, nil, true},
			{arrayV, nil, true},
			{docV, nil, true},
		})
	})

	t.Run("text", func(t *testing.T) {
		check(t, types.TextValue, []test{
			{boolV, types.NewTextValue("true"), false},
			{integerV, types.NewTextValue("10"), false},
			{doubleV, types.NewTextValue("10.5"), false},
			{textV, textV, false},
			{blobV, types.NewTextValue(`YXNkaW5l`), false},
			{arrayV, types.NewTextValue(`["bar", 10]`), false},
			{docV,
				types.NewTextValue(`{"a": 10, "b": "foo"}`),
				false},
		})
	})

	t.Run("blob", func(t *testing.T) {
		check(t, types.BlobValue, []test{
			{boolV, nil, true},
			{integerV, nil, true},
			{doubleV, nil, true},
			{types.NewTextValue("YXNkaW5l"), types.NewBlobValue([]byte{0x61, 0x73, 0x64, 0x69, 0x6e, 0x65}), false},
			{types.NewTextValue("not base64"), nil, true},
			{blobV, blobV, false},
			{arrayV, nil, true},
			{docV, nil, true},
		})
	})

	t.Run("array", func(t *testing.T) {
		check(t, types.ArrayValue, []test{
			{boolV, nil, true},
			{integerV, nil, true},
			{doubleV, nil, true},
			{types.NewTextValue(`["bar", 10]`), arrayV, false},
			{types.NewTextValue("abc"), nil, true},
			{blobV, nil, true},
			{arrayV, arrayV, false},
			{docV, nil, true},
		})
	})

	t.Run("document", func(t *testing.T) {
		check(t, types.DocumentValue, []test{
			{boolV, nil, true},
			{integerV, nil, true},
			{doubleV, nil, true},
			{types.NewTextValue(`{"a": 10, "b": "foo"}`), docV, false},
			{types.NewTextValue("abc"), nil, true},
			{blobV, nil, true},
			{arrayV, nil, true},
			{docV, docV, false},
		})
	})
}
