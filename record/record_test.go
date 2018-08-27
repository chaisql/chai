package record

import (
	"testing"

	"github.com/asdine/genji/field"
	"github.com/stretchr/testify/require"
)

func TestFieldBuffer(t *testing.T) {
	f := FieldBuffer([]*field.Field{
		field.NewInt64("a", 10),
		field.NewString("b", "hello"),
	})

	c := f.Cursor()
	require.Panics(t, func() {
		c.Field()
	})

	var i int
	for c.Next() {
		field, err := c.Field()
		require.NoError(t, err)
		require.Equal(t, field, f[i])
		i++
	}

	require.Equal(t, 2, i)
}
