package object_test

import (
	"encoding/json"
	"testing"

	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/object"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func TestArrayContains(t *testing.T) {
	arr := object.NewValueBuffer(
		types.NewIntegerValue(1),
		types.NewTextValue("foo"),
		types.NewBlobValue([]byte{1, 2, 3}),
	)

	ok, err := object.ArrayContains(arr, types.NewDoubleValue(1))
	assert.NoError(t, err)
	require.True(t, ok)

	ok, err = object.ArrayContains(arr, types.NewTextValue("foo"))
	assert.NoError(t, err)
	require.True(t, ok)

	ok, err = object.ArrayContains(arr, types.NewTextValue("bar"))
	assert.NoError(t, err)
	require.False(t, ok)
}

func TestValueBufferCopy(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{"empty array", `[]`},
		{"flat", `[1.4,-5,"hello",true]`},
		{"nested", `[["foo","bar",1],{"a":1},[1,2]]`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var from, to object.ValueBuffer
			assert.NoError(t, from.UnmarshalJSON([]byte(test.want)))
			err := to.Copy(&from)
			assert.NoError(t, err)
			got, err := json.Marshal(to)
			assert.NoError(t, err)
			require.Equal(t, test.want, string(got))
		})
	}
}

func TestValueBufferApply(t *testing.T) {
	var buf object.ValueBuffer
	err := buf.UnmarshalJSON([]byte(`[1, [1, 3], {"4": 5}]`))
	assert.NoError(t, err)

	err = buf.Apply(func(p object.Path, v types.Value) (types.Value, error) {
		return types.NewIntegerValue(6), nil
	})
	assert.NoError(t, err)

	got, err := json.Marshal(buf)
	assert.NoError(t, err)
	require.JSONEq(t, `[6, [6, 6], {"4": 6}]`, string(got))
}
