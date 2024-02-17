package expr_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/testutil/assert"
	"github.com/chaisql/chai/internal/types"
	"github.com/stretchr/testify/require"
)

var nullLiteral = types.NewNullValue()

func TestString(t *testing.T) {
	var operands = []string{
		`10.4`,
		"true",
		"500",
		`foo`,
		`"hello"`,
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
		want := fmt.Sprintf("10.4 %s foo", op)
		testFn(want, want)
	}
}
