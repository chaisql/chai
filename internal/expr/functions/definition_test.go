package functions_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/expr/functions"
	"github.com/stretchr/testify/require"
)

func TestFunctionDef(t *testing.T) {
	table := functions.DefaultPackagesTable()
	def, err := table.GetFunc("", "count")
	require.NoError(t, err)

	t.Run("String()", func(t *testing.T) {
		require.Equal(t, "count(arg1)", def.String())
	})

	t.Run("Function()", func(t *testing.T) {
		fexpr, err := def.Function(expr.Path(document.NewPath("a")))
		require.NoError(t, err)
		require.NotNil(t, fexpr)
	})

	t.Run("Arity()", func(t *testing.T) {
		require.Equal(t, 1, def.Arity())
	})
}

func TestPackagesTable(t *testing.T) {
	table := functions.DefaultPackagesTable()

	t.Run("OK GetFunc()", func(t *testing.T) {
		def, err := table.GetFunc("math", "floor")
		require.NoError(t, err)
		require.Equal(t, "floor", def.Name())
		def, err = table.GetFunc("", "count")
		require.NoError(t, err)
		require.Equal(t, "count", def.Name())
	})

	t.Run("NOK GetFunc() missing func", func(t *testing.T) {
		def, err := table.GetFunc("math", "foobar")
		require.Error(t, err)
		require.Nil(t, def)
		def, err = table.GetFunc("", "foobar")
		require.Error(t, err)
		require.Nil(t, def)
	})

	t.Run("NOK GetFunc() missing package", func(t *testing.T) {
		def, err := table.GetFunc("foobar", "foobar")
		require.Error(t, err)
		require.Nil(t, def)
	})
}
