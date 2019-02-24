package record

import (
	"testing"

	"github.com/asdine/genji/field"
	"github.com/stretchr/testify/require"
)

func TestFieldBuffer(t *testing.T) {
	b := FieldBuffer([]field.Field{
		field.NewInt64("a", 10),
		field.NewString("b", "hello"),
	})

	var i int
	err := b.Iterate(func(f field.Field) error {
		require.NotEmpty(t, f)
		require.Equal(t, f, b[i])
		i++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, i)
}
