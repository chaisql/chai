package types_test

import (
	"math"
	"testing"
	"time"

	"github.com/chaisql/chai/internal/types"
	"github.com/stretchr/testify/require"
)

func TestCastAs(t *testing.T) {
	type test struct {
		v, want types.Value
		fails   bool
	}
	now := time.Now()

	boolV := types.NewBooleanValue(true)
	integerV := types.NewIntegerValue(10)
	doubleV := types.NewDoubleValue(10.5)
	tsV := types.NewTimestampValue(now)
	textV := types.NewTextValue("foo")
	blobV := types.NewBlobValue([]byte("asdine"))

	check := func(t *testing.T, targetType types.Type, tests []test) {
		t.Helper()

		for _, test := range tests {
			t.Run(test.v.String(), func(t *testing.T) {
				t.Helper()

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
		check(t, types.TypeBoolean, []test{
			{boolV, boolV, false},
			{integerV, boolV, false},
			{types.NewIntegerValue(0), types.NewBooleanValue(false), false},
			{doubleV, nil, true},
			{textV, nil, true},
			{types.NewTextValue("true"), boolV, false},
			{types.NewTextValue("false"), types.NewBooleanValue(false), false},
			{blobV, nil, true},
		})
	})

	t.Run("integer", func(t *testing.T) {
		check(t, types.TypeInteger, []test{
			{boolV, types.NewIntegerValue(1), false},
			{types.NewBooleanValue(false), types.NewIntegerValue(0), false},
			{integerV, integerV, false},
			{doubleV, integerV, false},
			{textV, nil, true},
			{types.NewTextValue("10"), integerV, false},
			{types.NewTextValue("10.5"), integerV, false},
			{blobV, nil, true},
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
		})
	})

	t.Run("ts", func(t *testing.T) {
		check(t, types.TypeTimestamp, []test{
			{boolV, nil, true},
			{integerV, nil, true},
			{doubleV, nil, true},
			{types.NewTextValue(now.Format(time.RFC3339Nano)), tsV, false},
			{blobV, nil, true},
		})
	})

	t.Run("text", func(t *testing.T) {
		check(t, types.TypeText, []test{
			{boolV, types.NewTextValue("true"), false},
			{integerV, types.NewTextValue("10"), false},
			{doubleV, types.NewTextValue("10.5"), false},
			{textV, textV, false},
			{blobV, types.NewTextValue(`YXNkaW5l`), false},
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
		})
	})
}
