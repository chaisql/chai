package object

import (
	"math"
	"testing"
	"time"

	"github.com/chaisql/chai/internal/testutil/assert"
	"github.com/chaisql/chai/internal/types"
	"github.com/stretchr/testify/require"
)

func TestCastAs(t *testing.T) {
	type test struct {
		v, want types.Value
		fails   bool
	}
	now := time.Now()

	boolV := types.NewBoolValue(true)
	integerV := types.NewIntegerValue(10)
	doubleV := types.NewDoubleValue(10.5)
	tsV := types.NewTimestampValue(now)
	textV := types.NewTextValue("foo")
	blobV := types.NewBlobValue([]byte("asdine"))
	arrayV := types.NewArrayValue(NewValueBuffer().
		Append(types.NewTextValue("bar")).
		Append(integerV))
	docV := types.NewObjectValue(NewFieldBuffer().
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
		check(t, types.TypeBoolean, []test{
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
		check(t, types.TypeInteger, []test{
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
		check(t, types.TypeDouble, []test{
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

	t.Run("ts", func(t *testing.T) {
		check(t, types.TypeTimestamp, []test{
			{boolV, nil, true},
			{integerV, nil, true},
			{doubleV, nil, true},
			{types.NewTextValue(now.Format(time.RFC3339Nano)), tsV, false},
			{blobV, nil, true},
			{arrayV, nil, true},
			{docV, nil, true},
		})
	})

	t.Run("text", func(t *testing.T) {
		check(t, types.TypeText, []test{
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
		check(t, types.TypeBlob, []test{
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
		check(t, types.TypeArray, []test{
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

	t.Run("object", func(t *testing.T) {
		check(t, types.TypeObject, []test{
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
