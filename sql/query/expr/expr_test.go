package expr_test

import (
	"strings"
	"testing"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/sql/parser"
	"github.com/asdine/genji/sql/query/expr"
	"github.com/stretchr/testify/require"
)

var doc document.Document = func() document.Document {
	d, _ := document.NewFromJSON([]byte(`{
		"a": 1,
		"b": {"foo bar": [1, 2]},
		"c": [1, {"foo": "bar"}, [1, 2]]
	}`))

	return d
}()

var stackWithDoc = expr.EvalStack{
	Document: doc,
}

var fakeTableConfig = &database.TableConfig{
	FieldConstraints: []database.FieldConstraint{
		{Path: []string{"c", "0"}, IsPrimaryKey: true},
		{Path: []string{"c", "1"}},
	},
}
var stackWithDocAndConfig = expr.EvalStack{
	Document: doc,
	Cfg:      fakeTableConfig,
}

var nullLitteral = document.NewNullValue()

func testExpr(t testing.TB, exprStr string, stack expr.EvalStack, want document.Value, fails bool) {
	e, _, err := parser.NewParser(strings.NewReader(exprStr)).ParseExpr()
	require.NoError(t, err)
	res, err := e.Eval(stack)
	if fails {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
		require.Equal(t, want, res)
	}
}
