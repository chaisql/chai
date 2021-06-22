package expr_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/stretchr/testify/require"
)

func TestPkExpr(t *testing.T) {
	tests := []struct {
		name string
		env  *environment.Environment
		res  document.Value
	}{
		{"empty env", &environment.Environment{}, nullLiteral},
		{"env with doc", envWithDoc, nullLiteral},
		{"env with doc and key", envWithDocAndKey, document.NewIntegerValue(1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testExpr(t, "pk()", test.env, test.res, false)
		})
	}
}

func TestFunctionDef(t *testing.T) {
	table := expr.DefaultPackagesTable()
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
	table := expr.DefaultPackagesTable()

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
