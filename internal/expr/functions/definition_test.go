package functions_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/expr/functions"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/stretchr/testify/require"
)

var doc document.Document = func() document.Document {
	return document.NewFromJSON([]byte(`{
		"a": 1,
		"b": {"foo bar": [1, 2]},
		"c": [1, {"foo": "bar"}, [1, 2]]
	}`))
}()

var docWithKey document.Document = func() document.Document {
	fb := document.NewFieldBuffer()
	err := fb.Copy(doc)
	if err != nil {
		panic(err)
	}

	fb.DecodedKey = document.NewIntegerValue(1)
	fb.EncodedKey, err = fb.DecodedKey.MarshalBinary()
	if err != nil {
		panic(err)
	}

	return fb
}()

var envWithDoc = environment.New(doc)
var envWithDocAndKey = environment.New(docWithKey)

func TestPkExpr(t *testing.T) {
	tests := []struct {
		name string
		env  *environment.Environment
		res  document.Value
	}{
		{"empty env", &environment.Environment{}, document.NewNullValue()},
		{"env with doc", envWithDoc, document.NewNullValue()},
		{"env with doc and key", envWithDocAndKey, document.NewIntegerValue(1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testutil.TestExpr(t, "pk()", test.env, test.res, false)
		})
	}
}

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
