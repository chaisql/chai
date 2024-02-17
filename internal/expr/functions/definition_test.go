package functions_test

import (
	"testing"

	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/expr/functions"
	"github.com/chaisql/chai/internal/testutil/assert"
	"github.com/stretchr/testify/require"
)

func TestDefinitions(t *testing.T) {
	def, err := functions.GetFunc("count")
	assert.NoError(t, err)

	t.Run("String()", func(t *testing.T) {
		require.Equal(t, "count(arg1)", def.String())
	})

	t.Run("Function()", func(t *testing.T) {
		fexpr, err := def.Function(expr.Column("a"))
		assert.NoError(t, err)
		require.NotNil(t, fexpr)
	})
}
