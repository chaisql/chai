package testutil

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/expr/functions"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/sql/scanner"
	"github.com/chaisql/chai/internal/testutil/genexprtests"
	"github.com/chaisql/chai/internal/types"
	"github.com/stretchr/testify/require"
)

// NullValue creates a literal value of type Null.
func NullValue() expr.LiteralValue {
	return expr.LiteralValue{Value: types.NewNullValue()}
}

// BoolValue creates a literal value of type Bool.
func BoolValue(v bool) expr.LiteralValue {
	return expr.LiteralValue{Value: types.NewBooleanValue(v)}
}

// IntegerValue creates a literal value of type Integer.
func IntegerValue(v int32) expr.LiteralValue {
	return expr.LiteralValue{Value: types.NewIntegerValue(v)}
}

// BigintValue creates a literal value of type Bigint.
func BigintValue(v int64) expr.LiteralValue {
	return expr.LiteralValue{Value: types.NewBigintValue(v)}
}

// DoubleValue creates a literal value of type Double.
func DoubleValue(v float64) expr.LiteralValue {
	return expr.LiteralValue{Value: types.NewDoublePrecisionValue(v)}
}

// TextValue creates a literal value of type Text.
func TextValue(v string) expr.LiteralValue {
	return expr.LiteralValue{Value: types.NewTextValue(v)}
}

// ByteaValue creates a literal value of type bytea.
func ByteaValue(v []byte) expr.LiteralValue {
	return expr.LiteralValue{Value: types.NewByteaValue(v)}
}

func ExprList(t testing.TB, s string) expr.LiteralExprList {
	t.Helper()

	e, err := parser.ParseExpr(s)
	require.NoError(t, err)
	switch e := e.(type) {
	case expr.LiteralExprList:
		return e
	case expr.Parentheses:
		return expr.LiteralExprList{e.E}
	default:
		t.Fatalf("unexpected expression type: %T", e)
	}

	return nil
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
	require.NoError(t, err)

	return e
}

func ParseExprs(t testing.TB, s ...string) []expr.Expr {
	t.Helper()

	ex := make([]expr.Expr, len(s))
	for i, e := range s {
		ex[i] = ParseExpr(t, e)
	}

	return ex
}

func ParseExprList(t testing.TB, s string) expr.LiteralExprList {
	t.Helper()

	e, err := parser.ParseExpr(s)
	require.NoError(t, err)

	switch e := e.(type) {
	case expr.LiteralExprList:
		return e
	case expr.Parentheses:
		return expr.LiteralExprList{e.E}
	default:
		t.Fatalf("unexpected expression type: %T", e)
	}

	return e.(expr.LiteralExprList)
}

func TestExpr(t testing.TB, exprStr string, env *environment.Environment, want types.Value, fails bool) {
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

func FunctionExpr(t testing.TB, name string, args ...expr.Expr) expr.Expr {
	t.Helper()
	def, err := functions.GetFunc(name)
	require.NoError(t, err)
	require.NotNil(t, def)
	expr, err := def.Function(args...)
	require.NoError(t, err)
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
	require.NoError(t, err)

	tx := database.Transaction{
		TxStart: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	env := environment.New(nil, &tx, nil, nil)

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
						require.NoErrorf(t, err, "parse error at %s:%d\n`%s`: %v", testfile, stmt.ResLine, stmt.Res, err)

						// eval it to get a proper Value
						want, err := e.Eval(env)
						require.NoErrorf(t, err, "eval error at %s:%d\n`%s`: %v", testfile, stmt.ResLine, stmt.Res, err)

						// parse the given expr
						p := parser.NewParser(strings.NewReader(stmt.Expr))
						e, err = p.ParseExpr()
						require.NoErrorf(t, err, "parse error at %s:%d\n`%s`: %v", testfile, stmt.ExprLine, stmt.Expr, err)
						tok, _, _ := p.ScanIgnoreWhitespace()
						if tok != scanner.EOF {
							t.Fatalf("expected EOF, got %s", tok)
						}
						// eval it to get a proper Value
						got, err := e.Eval(env)
						require.NoErrorf(t, err, "eval error at %s:%d\n`%s`: %v", testfile, stmt.ExprLine, stmt.Expr, err)

						// finally, compare those two
						RequireValueEqual(t, want, got, "assertion error at %s:%d", testfile, stmt.ResLine)
					})
				} else {
					t.Run("NOK "+stmt.Expr, func(t *testing.T) {
						t.Helper()
						// parse the given expr
						p := parser.NewParser(strings.NewReader(stmt.Expr))
						e, err := p.ParseExpr()
						if err != nil {
							require.Regexp(t, regexp.MustCompile(regexp.QuoteMeta(stmt.Res)), err.Error())
						} else {
							tok, _, _ := p.ScanIgnoreWhitespace()
							if tok != scanner.EOF {
								return
							}
							// eval it, it should return an error
							_, err = e.Eval(env)
							require.NotNilf(t, err, "expected expr to return an error at %s:%d\n`%s`, got nil", testfile, stmt.ExprLine, stmt.Expr)
							require.Regexpf(t, regexp.MustCompile(regexp.QuoteMeta(stmt.Res)), err.Error(), "expected error message to match at %s:%d", testfile, stmt.ResLine)
						}
					})
				}
			}
		})
	}
}
