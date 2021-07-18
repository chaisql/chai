package document

import (
	"encoding/json"
	"testing"

	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func TestArrayContains(t *testing.T) {
	arr := NewValueBuffer(
		types.NewIntegerValue(1),
		types.NewTextValue("foo"),
		types.NewBlobValue([]byte{1, 2, 3}),
	)

	ok, err := ArrayContains(arr, types.NewDoubleValue(1))
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = ArrayContains(arr, types.NewTextValue("foo"))
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = ArrayContains(arr, types.NewTextValue("bar"))
	require.NoError(t, err)
	require.False(t, ok)
}

func TestSortArray(t *testing.T) {
	tests := []struct {
		name     string
		arr      string
		expected string
	}{
		{"empty array", `[]`, `[]`},
		{"numbers", `[1.4,3,2.1,-5]`, `[-5,1.4,2.1,3]`},
		{"text", `["foo","bar",""]`, `["","bar","foo"]`},
		{"arrays", `[[1, 2],[-1,10],[]]`, `[[],[-1,10],[1,2]]`},
		{"documents", `[{"z":10},{"a":40},{}]`, `[{},{"a":40},{"z":10}]`},
		{"mixed", `["foo",["a"],{},null,true,10]`, `[null,true,10,"foo",["a"],{}]`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var arr ValueBuffer
			require.NoError(t, arr.UnmarshalJSON([]byte(test.arr)))
			output, err := SortArray(&arr)
			require.NoError(t, err)
			actual, err := json.Marshal(output)
			require.NoError(t, err)
			require.Equal(t, test.expected, string(actual))
		})
	}
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
			var from, to ValueBuffer
			require.NoError(t, from.UnmarshalJSON([]byte(test.want)))
			err := to.Copy(&from)
			require.NoError(t, err)
			got, err := json.Marshal(to)
			require.NoError(t, err)
			require.Equal(t, test.want, string(got))
		})
	}
}

func TestValueBufferApply(t *testing.T) {
	var buf ValueBuffer
	err := buf.UnmarshalJSON([]byte(`[1, [1, 3], {"4": 5}]`))
	require.NoError(t, err)

	err = buf.Apply(func(p Path, v types.Value) (types.Value, error) {
		return types.NewIntegerValue(6), nil
	})
	require.NoError(t, err)

	got, err := json.Marshal(buf)
	require.NoError(t, err)
	require.JSONEq(t, `[6, [6, 6], {"4": 6}]`, string(got))
}
