package expr_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
	"github.com/stretchr/testify/require"
)

func TestEnvironmentClone(t *testing.T) {
	d := document.NewFieldBuffer()
	d.Add("answer", document.NewIntegerValue(42))

	p := expr.Param{Name: "param", Value: 1}
	tx := &database.Transaction{}

	vars := document.NewFieldBuffer()
	vars.Add("var", document.NewIntegerValue(2))

	env := expr.NewEnvironment(d, p)
	outer := expr.NewEnvironment(nil)

	env.Tx = tx
	env.Outer = outer
	env.Vars = vars

	newEnv, err := env.Clone()
	require.NoError(t, err)

	require.Equal(t, d, newEnv.Doc)
	require.Equal(t, 1, len(newEnv.Params))
	require.Equal(t, p, newEnv.Params[0])
	require.Equal(t, tx, newEnv.Tx)
	require.Equal(t, vars, newEnv.Vars)
	require.Equal(t, outer, newEnv.Outer)
}
