package field_test

import (
	"testing"

	"github.com/asdine/genji/field"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeInt64(t *testing.T) {
	v := field.EncodeInt64(-100)
	i, err := field.DecodeInt64(v)
	require.NoError(t, err)
	require.Equal(t, int64(-100), i)

	t.Run("Overflow", func(t *testing.T) {
		v := make([]byte, 10)
		for i := 0; i < 10; i++ {
			v[i] = 255
		}

		_, err := field.DecodeInt64(v)
		require.Error(t, err)
	})
}
