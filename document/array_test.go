package document

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArrayContains(t *testing.T) {
	arr := NewValueBuffer(
		NewIntValue(1),
		NewTextValue("foo"),
		NewBlobValue([]byte{1, 2, 3}),
	)

	ok, err := ArrayContains(arr, NewFloat64Value(1))
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = ArrayContains(arr, NewBlobValue([]byte("foo")))
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = ArrayContains(arr, NewBlobValue([]byte("bar")))
	require.NoError(t, err)
	require.False(t, ok)
}
