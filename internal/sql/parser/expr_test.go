package parser_test

import (
	"strings"
	"testing"

	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/expr/functions"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/testutil/assert"
	"github.com/chaisql/chai/internal/types"
	"github.com/stretchr/testify/require"
)

func TestParserExpr(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected expr.Expr
		fails    bool
	}{
		// integers
		{"int", "10", testutil.IntegerValue(10), false},
		{"-int", "-10", testutil.IntegerValue(-10), false},
		{"+int", "+10", testutil.IntegerValue(10), false},
		{"> max int64 -> float64", "10000000000000000000", testutil.DoubleValue(10000000000000000000), false},
		{"< min int64 -> float64", "-10000000000000000000", testutil.DoubleValue(-10000000000000000000), false},
		{"very large int", "100000000000000000000000000000000000000000000000", testutil.DoubleValue(100000000000000000000000000000000000000000000000), false},

		// floats
		{"+float64", "10.0", testutil.DoubleValue(10), false},
		{"-float64", "-10.0", testutil.DoubleValue(-10), false},

		// strings
		{"double quoted string", `"10.0"`, testutil.TextValue("10.0"), false},
		{"single quoted string", "'-10.0'", testutil.TextValue("-10.0"), false},

		// blobs
		{"blob as hex string", `'\xff'`, testutil.BlobValue([]byte{255}), false},
		{"invalid blob hex string", `'\xzz'`, nil, true},

		// parentheses
		{"parentheses: empty", "()", nil, true},
		{"parentheses: values", `(1)`,
			expr.Parentheses{
				E: testutil.IntegerValue(1),
			}, false},
		{"parentheses: expr", `(1 + true * (4 + 3))`,
			expr.Parentheses{
				E: expr.Add(
					testutil.IntegerValue(1),
					expr.Mul(
						testutil.BoolValue(true),
						expr.Parentheses{
							E: expr.Add(
								testutil.IntegerValue(4),
								testutil.IntegerValue(3),
							),
						},
					),
				),
			}, false},

		// operators
		{"=", "age = 10", expr.Eq(expr.Column("age"), testutil.IntegerValue(10)), false},
		{"!=", "age != 10", expr.Neq(expr.Column("age"), testutil.IntegerValue(10)), false},
		{">", "age > 10", expr.Gt(expr.Column("age"), testutil.IntegerValue(10)), false},
		{">=", "age >= 10", expr.Gte(expr.Column("age"), testutil.IntegerValue(10)), false},
		{"<", "age < 10", expr.Lt(expr.Column("age"), testutil.IntegerValue(10)), false},
		{"<=", "age <= 10", expr.Lte(expr.Column("age"), testutil.IntegerValue(10)), false},
		{"BETWEEN", "1 BETWEEN 10 AND 11", expr.Between(testutil.IntegerValue(10))(testutil.IntegerValue(1), testutil.IntegerValue(11)), false},
		{"+", "age + 10", expr.Add(expr.Column("age"), testutil.IntegerValue(10)), false},
		{"-", "age - 10", expr.Sub(expr.Column("age"), testutil.IntegerValue(10)), false},
		{"*", "age * 10", expr.Mul(expr.Column("age"), testutil.IntegerValue(10)), false},
		{"/", "age / 10", expr.Div(expr.Column("age"), testutil.IntegerValue(10)), false},
		{"%", "age % 10", expr.Mod(expr.Column("age"), testutil.IntegerValue(10)), false},
		{"&", "age & 10", expr.BitwiseAnd(expr.Column("age"), testutil.IntegerValue(10)), false},
		{"||", "name || 'foo'", expr.Concat(expr.Column("name"), testutil.TextValue("foo")), false},
		{"IN", "age IN ages", expr.In(expr.Column("age"), expr.Column("ages")), false},
		{"NOT IN", "age NOT IN ages", expr.NotIn(expr.Column("age"), expr.Column("ages")), false},
		{"IS", "age IS NULL", expr.Is(expr.Column("age"), testutil.NullValue()), false},
		{"IS NOT", "age IS NOT NULL", expr.IsNot(expr.Column("age"), testutil.NullValue()), false},
		{"LIKE", "name LIKE 'foo'", expr.Like(expr.Column("name"), testutil.TextValue("foo")), false},
		{"NOT LIKE", "name NOT LIKE 'foo'", expr.NotLike(expr.Column("name"), testutil.TextValue("foo")), false},
		{"NOT =", "name NOT = 'foo'", nil, true},
		{"precedence", "4 > 1 + 2", expr.Gt(
			testutil.IntegerValue(4),
			expr.Add(
				testutil.IntegerValue(1),
				testutil.IntegerValue(2),
			),
		), false},
		{"AND", "age = 10 AND age <= 11",
			expr.And(
				expr.Eq(expr.Column("age"), testutil.IntegerValue(10)),
				expr.Lte(expr.Column("age"), testutil.IntegerValue(11)),
			), false},
		{"OR", "age = 10 OR age = 11",
			expr.Or(
				expr.Eq(expr.Column("age"), testutil.IntegerValue(10)),
				expr.Eq(expr.Column("age"), testutil.IntegerValue(11)),
			), false},
		{"AND then OR", "age >= 10 AND age > $age OR age < 10.4",
			expr.Or(
				expr.And(
					expr.Gte(expr.Column("age"), testutil.IntegerValue(10)),
					expr.Gt(expr.Column("age"), expr.NamedParam("age")),
				),
				expr.Lt(expr.Column("age"), testutil.DoubleValue(10.4)),
			), false},
		{"with NULL", "age > NULL", expr.Gt(expr.Column("age"), testutil.NullValue()), false},

		// unary operators
		{"CAST", "CAST(a AS TEXT)", &expr.Cast{Expr: expr.Column("a"), CastAs: types.TypeText}, false},
		{"NOT", "NOT 10", expr.Not(testutil.IntegerValue(10)), false},
		{"NOT", "NOT NOT", nil, true},
		{"NOT", "NOT NOT 10", expr.Not(expr.Not(testutil.IntegerValue(10))), false},
		{"NEXT VALUE FOR", "NEXT VALUE FOR hello", expr.NextValueFor{SeqName: "hello"}, false},
		{"NEXT VALUE FOR", "NEXT VALUE FOR `good morning`", expr.NextValueFor{SeqName: "good morning"}, false},
		{"NEXT VALUE FOR", "NEXT VALUE FOR 10", nil, true},

		// functions
		{"count(expr) function", "count(a)", &functions.Count{Expr: expr.Column("a")}, false},
		{"count(*) function", "count(*)", functions.NewCount(expr.Wildcard{}), false},
		{"count (*) function with spaces", "count      (*)", functions.NewCount(expr.Wildcard{}), false},
		{"packaged function", "floor(1.2)", testutil.FunctionExpr(t, "floor", testutil.DoubleValue(1.2)), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ex, err := parser.NewParser(strings.NewReader(test.s)).ParseExpr()
			if test.fails {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if !expr.Equal(test.expected, ex) {
					require.EqualValues(t, test.expected, ex)
				}
			}
		})
	}
}

func TestParserParams(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected expr.Expr
		errored  bool
	}{
		{"one positional", "age = ?", expr.Eq(expr.Column("age"), expr.PositionalParam(1)), false},
		{"multiple positional", "age = ? AND age <= ?",
			expr.And(
				expr.Eq(expr.Column("age"), expr.PositionalParam(1)),
				expr.Lte(expr.Column("age"), expr.PositionalParam(2)),
			), false},
		{"one named", "age = $age", expr.Eq(expr.Column("age"), expr.NamedParam("age")), false},
		{"multiple named", "age = $foo OR age = $bar",
			expr.Or(
				expr.Eq(expr.Column("age"), expr.NamedParam("foo")),
				expr.Eq(expr.Column("age"), expr.NamedParam("bar")),
			), false},
		{"mixed", "age >= ? AND age > $foo OR age < ?", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ex, err := parser.NewParser(strings.NewReader(test.s)).ParseExpr()
			if test.errored {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				require.EqualValues(t, test.expected, ex)
			}
		})
	}
}
