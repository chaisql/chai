package functions_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/expr/functions"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func TestScalarFunctionDef(t *testing.T) {
	def := functions.NewScalarDefinition(
		"foo",
		3,
		func(args ...types.Value) (types.Value, error) {
			arg1 := args[0].V().(int64)
			arg2 := args[1].V().(int64)
			arg3 := args[2].V().(int64)

			return types.NewIntegerValue(arg1 + arg2 + arg3), nil
		},
	)

	t.Run("Name()", func(t *testing.T) {
		require.Equal(t, "foo", def.Name())
	})

	t.Run("Arity()", func(t *testing.T) {
		require.Equal(t, 3, def.Arity())
	})

	t.Run("String()", func(t *testing.T) {
		require.Equal(t, "foo(arg1, arg2, arg3)", def.String())
	})

	t.Run("Function()", func(t *testing.T) {
		fb := document.NewFieldBuffer()
		fb = fb.Add("a", types.NewIntegerValue(2))
		env := environment.New(fb)
		expr1 := expr.Add(expr.LiteralValue{Value: types.NewIntegerValue(1)}, expr.LiteralValue{Value: types.NewIntegerValue(0)})
		expr2 := expr.Path(document.NewPath("a"))
		expr3 := expr.Div(expr.LiteralValue{Value: types.NewIntegerValue(6)}, expr.LiteralValue{Value: types.NewIntegerValue(2)})

		t.Run("OK", func(t *testing.T) {
			fexpr, err := def.Function(expr1, expr2, expr3)
			assert.NoError(t, err)
			v, err := fexpr.Eval(env)
			assert.NoError(t, err)
			require.Equal(t, types.NewIntegerValue(1+2+3), v)
		})

		t.Run("NOK", func(t *testing.T) {
			_, err := def.Function(expr1, expr2)
			assert.Error(t, err)
		})
	})
}
