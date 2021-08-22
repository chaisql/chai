package testutil

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/expr/functions"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/internal/testutil/genexprtests"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

// BlobValue creates a literal value of type Blob.
func BlobValue(v []byte) expr.LiteralValue {
	return expr.LiteralValue{Value: types.NewBlobValue(v)}
}

// TextValue creates a literal value of type Text.
func TextValue(v string) expr.LiteralValue {
	return expr.LiteralValue{Value: types.NewTextValue(v)}
}

// BoolValue creates a literal value of type Bool.
func BoolValue(v bool) expr.LiteralValue {
	return expr.LiteralValue{Value: types.NewBoolValue(v)}
}

// IntegerValue creates a literal value of type Integer.
func IntegerValue(v int64) expr.LiteralValue {
	return expr.LiteralValue{Value: types.NewIntegerValue(v)}
}

// DoubleValue creates a literal value of type Double.
func DoubleValue(v float64) expr.LiteralValue {
	return expr.LiteralValue{Value: types.NewDoubleValue(v)}
}

// NullValue creates a literal value of type Null.
func NullValue() expr.LiteralValue {
	return expr.LiteralValue{Value: types.NewNullValue()}
}

// DocumentValue creates a literal value of type Document.
func DocumentValue(d types.Document) expr.LiteralValue {
	return expr.LiteralValue{Value: types.NewDocumentValue(d)}
}

// ArrayValue creates a literal value of type Array.
func ArrayValue(a types.Array) expr.LiteralValue {
	return expr.LiteralValue{Value: types.NewArrayValue(a)}
}

func ExprList(t testing.TB, s string) expr.LiteralExprList {
	t.Helper()

	e, err := parser.ParseExpr(s)
	assert.NoError(t, err)
	require.IsType(t, e, expr.LiteralExprList{})

	return e.(expr.LiteralExprList)
}

func ParsePath(t testing.TB, p string) expr.Path {
	t.Helper()

	return expr.Path(ParseDocumentPath(t, p))
}

func ParseDocumentPath(t testing.TB, p string) document.Path {
	t.Helper()

	vp, err := parser.ParsePath(p)
	assert.NoError(t, err)
	return vp
}

func ParseNamedExpr(t testing.TB, s string, name ...string) expr.Expr {
	t.Helper()

	ne := expr.NamedExpr{
		Expr:     ParseExpr(t, s),
		ExprName: s,
	}

	if len(name) > 0 {
		ne.ExprName = name[0]
	}

	return &ne
}

func ParseExpr(t testing.TB, s string) expr.Expr {
	t.Helper()

	e, err := parser.ParseExpr(s)
	assert.NoError(t, err)

	return e
}

func TestExpr(t testing.TB, exprStr string, env *environment.Environment, want types.Value, fails bool) {
	t.Helper()
	e, err := parser.NewParser(strings.NewReader(exprStr)).ParseExpr()
	assert.NoError(t, err)
	res, err := e.Eval(env)
	if fails {
		assert.Error(t, err)
	} else {
		assert.NoError(t, err)
		require.Equal(t, want, res)
	}
}

func FunctionExpr(t testing.TB, name string, args ...expr.Expr) expr.Expr {
	t.Helper()
	n := strings.Split(name, ".")
	def, err := functions.DefaultPackages().GetFunc(n[0], n[1])
	assert.NoError(t, err)
	require.NotNil(t, def)
	expr, err := def.Function(args...)
	assert.NoError(t, err)
	require.NotNil(t, expr)
	return expr
}

func ExprRunner(t *testing.T, testfile string) {
	t.Helper()

	f, err := os.Open(testfile)
	if err != nil {
		t.Errorf("Failed to open test data, got %v (%s)", err, testfile)
	}

	ts, err := genexprtests.Parse(f)
	assert.NoError(t, err)

	for _, test := range ts.Tests {
		t.Run(test.Name, func(t *testing.T) {
			t.Helper()
			testfile, _ := filepath.Abs(testfile)
			for _, stmt := range test.Statements {
				if !stmt.Fail {
					t.Run("OK "+stmt.Expr, func(t *testing.T) {
						t.Helper()
						// parse the expected result
						e, err := parser.NewParser(strings.NewReader(stmt.Res)).ParseExpr()
						assert.NoErrorf(t, err, "parse error at %s:%d\n`%s`", testfile, stmt.ResLine, stmt.Res)

						// eval it to get a proper Value
						want, err := e.Eval(environment.New(nil))
						assert.NoErrorf(t, err, "eval error at %s:%d\n`%s`", testfile, stmt.ResLine, stmt.Res)

						// parse the given expr
						e, err = parser.NewParser(strings.NewReader(stmt.Expr)).ParseExpr()
						assert.NoErrorf(t, err, "parse error at %s:%d\n`%s`", testfile, stmt.ExprLine, stmt.Expr)

						// eval it to get a proper Value
						got, err := e.Eval(environment.New(nil))
						assert.NoErrorf(t, err, "eval error at %s:%d\n`%s`", testfile, stmt.ExprLine, stmt.Expr)

						// finally, compare those two
						require.Equalf(t, want, got, "assertion error at %s:%d", testfile, stmt.ResLine)
					})
				} else {
					t.Run("NOK "+stmt.Expr, func(t *testing.T) {
						t.Helper()
						// parse the given epxr
						e, err := parser.NewParser(strings.NewReader(stmt.Expr)).ParseExpr()
						if err != nil {
							require.Regexp(t, regexp.MustCompile(regexp.QuoteMeta(stmt.Res)), err.Error())
						} else {
							// eval it, it should return an error
							_, err = e.Eval(environment.New(nil))
							require.NotNilf(t, err, "expected expr to return an error at %s:%\n`%s`, got nil", testfile, stmt.ExprLine, stmt.Expr)
							require.Regexpf(t, regexp.MustCompile(regexp.QuoteMeta(stmt.Res)), err.Error(), "expected error message to match at %s:%d", testfile, stmt.ResLine)
						}
					})
				}
			}
		})
	}
}
