package expr_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/sql/parser"
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

var envWithDoc = expr.NewEnvironment(doc)

var envWithDocAndKey = expr.NewEnvironment(docWithKey)

var nullLitteral = document.NewNullValue()

func testExpr(t testing.TB, exprStr string, env *expr.Environment, want document.Value, fails bool) {
	t.Helper()

	e, err := parser.NewParser(strings.NewReader(exprStr)).ParseExpr()
	require.NoError(t, err)
	res, err := e.Eval(env)
	if fails {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
		require.Equal(t, want, res)
	}
}

func TestString(t *testing.T) {
	var operands = []string{
		`10.4`,
		"true",
		"500",
		`foo.bar[1]`,
		`"hello"`,
		`[1, 2, "foo"]`,
		`{"a": "foo", "b": 10}`,
		"pk()",
		"CAST(10 AS integer)",
	}

	var operators = []string{
		"=", ">", ">=", "<", "<=",
		"+", "-", "*", "/", "%", "&", "|", "^",
		"AND", "OR",
	}

	testFn := func(s string, want string) {
		e, err := parser.NewParser(strings.NewReader(s)).ParseExpr()
		require.NoError(t, err)
		require.Equal(t, want, fmt.Sprintf("%v", e))
	}

	for _, op := range operands {
		testFn(op, op)
	}

	for _, op := range operators {
		want := fmt.Sprintf("10.4 %s foo.bar[1]", op)
		testFn(want, want)
	}
}
