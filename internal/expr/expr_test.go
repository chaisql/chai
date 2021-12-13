package expr_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

var doc types.Document = func() types.Document {
	return document.NewFromJSON([]byte(`{
		"a": 1,
		"b": {"foo bar": [1, 2]},
		"c": [1, {"foo": "bar"}, [1, 2]]
	}`))
}()

var envWithDoc = environment.New(doc)

var nullLiteral = types.NewNullValue()

func TestString(t *testing.T) {
	var operands = []string{
		`10.4`,
		"true",
		"500",
		`foo.bar[1]`,
		`"hello"`,
		`[1, 2, "foo"]`,
		`{a: "foo", b: 10}`,
		"pk()",
		"CAST(10 AS integer)",
	}

	var operators = []string{
		"=", ">", ">=", "<", "<=",
		"+", "-", "*", "/", "%", "&", "|", "^",
		"AND", "OR",
	}

	testFn := func(s string, want string) {
		t.Helper()
		e, err := parser.NewParser(strings.NewReader(s)).ParseExpr()
		assert.NoError(t, err)
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
