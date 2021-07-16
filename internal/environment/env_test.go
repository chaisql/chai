package environment_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/catalog"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func TestEnvironmentClone(t *testing.T) {
	d := document.NewFieldBuffer()
	d.Add("answer", types.NewIntegerValue(42))

	p := environment.Param{Name: "param", Value: 1}
	tx := &database.Transaction{}
	catalog := catalog.New()

	vars := document.NewFieldBuffer()
	vars.Add("var", types.NewIntegerValue(2))

	var env, outer environment.Environment

	env.Doc = d
	env.Params = []environment.Param{p}
	env.Catalog = catalog
	env.Tx = tx
	env.Catalog = catalog
	env.Outer = &outer
	env.Vars = vars

	newEnv, err := env.Clone()
	require.NoError(t, err)

	require.Equal(t, d, newEnv.Doc)
	require.Equal(t, 1, len(newEnv.Params))
	require.Equal(t, p, newEnv.Params[0])
	require.Equal(t, tx, newEnv.Tx)
	require.Equal(t, catalog, newEnv.Catalog)
	require.Equal(t, vars, newEnv.Vars)
	require.Equal(t, &outer, newEnv.Outer)
}
